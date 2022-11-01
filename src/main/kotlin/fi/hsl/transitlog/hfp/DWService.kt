package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.azure.BlobUploader
import fi.hsl.transitlog.hfp.domain.Event
import fi.hsl.transitlog.hfp.domain.EventType
import fi.hsl.transitlog.hfp.domain.LightPriorityEvent
import fi.hsl.transitlog.hfp.utils.DaemonThreadFactory
import jdk.nashorn.internal.runtime.regexp.joni.Config.log
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.collections.ArrayList
import kotlin.math.min
import kotlin.math.roundToInt
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

@ExperimentalTime
class DWService(private val dataDirectory: Path, private val compressionLevel: Int, blobUploader: BlobUploader, privateBlobUploader: BlobUploader, msgAcknowledger: (MessageId) -> Unit) {
    companion object {
        private const val MAX_QUEUE_SIZE = 750_000
    }

    private val log = KotlinLogging.logger {}

    //Count how many times we have tried to upload data but there was nothing to upload
    //If this value grows too high, print debug information
    private var noUploadCounter = 0

    private val fileWriterExecutorService = Executors.newFixedThreadPool((Runtime.getRuntime().availableProcessors() * 1.5).roundToInt(), DaemonThreadFactory)
    private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

    private val messageQueue = LinkedBlockingQueue<Pair<Hfp.Data, MessageId>>(MAX_QUEUE_SIZE)

    private val msgIds = ConcurrentHashMap<Path, MutableList<MessageId>>()
    private val dwFiles = mutableMapOf<String, DWFile>()

    private inner class DWFileWriterRunnable(private val dwFile: DWFile, private val messages: List<Pair<Hfp.Data, MessageId>>) : Runnable {
        override fun run() {
            val msgIdList = msgIds.computeIfAbsent(dwFile.path) { LinkedList<MessageId>() }

            messages.forEach { (hfpData, msgId) ->
                val eventType = EventType.getEventType(hfpData.topic)
                if (eventType == EventType.LightPriorityEvent) {
                    dwFile.writeEvent(LightPriorityEvent.parse(hfpData.topic, hfpData.payload))
                } else {
                    dwFile.writeEvent(Event.parse(hfpData.topic, hfpData.payload))
                }

                msgIdList.add(msgId)
            }
        }
    }

    init {
        //Setup task for writing events to files
        scheduledExecutorService.scheduleWithFixedDelay({
            //Poll up to MAX_QUEUE_SIZE events from queue
            val messages = ArrayList<Pair<Hfp.Data, MessageId>>(min(MAX_QUEUE_SIZE, messageQueue.size))
            for (i in 1..MAX_QUEUE_SIZE) {
                val msg = messageQueue.poll()
                if (msg == null) {
                    break
                } else {
                    messages += msg
                }
            }

            log.debug { "Writing ${messages.size} messages to CSV files" }

            val messagesByFile = messages.groupBy { (hfpData, _) -> getDWFile(hfpData) }
            //Write messages to files
            val completionService = ExecutorCompletionService<Void>(fileWriterExecutorService)
            messagesByFile.map { (dwFile, messages) -> completionService.submit(DWFileWriterRunnable(dwFile, messages), null) }

            //Wait for writing to be done
            val duration = measureTime {
                try {
                    completionService.take().get()
                } catch (e: Exception) {
                    log.warn { "Exception while waiting for messages to be written: $e" }
                }
            }
            log.info { "Wrote ${messages.size} messages to CSV files in ${duration.inWholeMilliseconds} ms" }
        }, 15, 15, TimeUnit.SECONDS)

        //Setup task for uploading files to Azure every 15 minutes
        val timeBetweenUploads = Duration.ofMinutes(15)

        val initialDelay = getInitialDelayForUpload(timeBetweenUploads)

        scheduledExecutorService.scheduleAtFixedRate({
            val dwFilesCopy = dwFiles.toMap()

            log.info { "Uploading files to blob storage" }

            var filesUploaded = 0

            for ((key: String, dwFile: DWFile) in dwFilesCopy.entries) {
                if (dwFile.isReadyForUpload()) {
                    try {
                        //Close file for writing
                        dwFile.close()

                        //Upload file to Blob Storage
                        (if (dwFile.private) { privateBlobUploader } else { blobUploader }).uploadFromFile(dwFile.path, blobName = dwFile.blobName, metadata = dwFile.getMetadata())
                        
                        //Acknowledge all messages that were in the file
                        val ackMsgIds = msgIds[dwFile.path]!!
                        log.info { "Acknowledging ${ackMsgIds.size} messages which were written to file ${dwFile.path}" }
                        ackMsgIds.forEach(msgAcknowledger)
                        log.debug { "Messages written to ${dwFile.path} acknowledged" }

                        msgIds.remove(dwFile.path)
                        dwFiles.remove(key)

                        Files.delete(dwFile.path)

                        filesUploaded++
                    } catch (e: Exception) {
                        log.error(e) {
                            "Failed to upload file to Blob Storage"
                        }
                    }
                }
            }

            if (filesUploaded == 0) { noUploadCounter++ } else { noUploadCounter = 0 }

            if (noUploadCounter > 2) {
                val filesList = dwFilesCopy.values.joinToString("\n") {
                    "${it.path} (${it.blobName}), last modified ${
                        it.getLastModifiedAgo().toMinutes()
                    }min ago"
                }
                log.warn { "No files have been uploaded in last 2 tries, list of files:\n${filesList}" }
            }

            log.info { "Done uploading files to blob storage" }
        }, initialDelay.seconds, timeBetweenUploads.seconds, TimeUnit.SECONDS)
    }

    private fun getInitialDelayForUpload(timeBetweenUploads: Duration): Duration {
        val now = ZonedDateTime.now()
        var initialUploadTime = now.withMinute(15)

        while (initialUploadTime.isBefore(now)) {
            initialUploadTime = initialUploadTime.plusNanos(timeBetweenUploads.toNanos())
        }

        return Duration.ofMillis(now.until(initialUploadTime, ChronoUnit.MILLIS))
    }

    private fun getDWFile(hfpData: Hfp.Data): DWFile = dwFiles.computeIfAbsent(DWFile.createBlobName(hfpData)) { DWFile.createDWFile(hfpData, dataDirectory = dataDirectory, compressionLevel) }

    fun addEvent(hfpData: Hfp.Data, msgId: MessageId) = messageQueue.put(hfpData to msgId)
}