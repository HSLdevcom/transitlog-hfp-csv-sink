package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.azure.BlobUploader
import fi.hsl.transitlog.hfp.domain.Event
import fi.hsl.transitlog.hfp.domain.EventType
import fi.hsl.transitlog.hfp.domain.LightPriorityEvent
import fi.hsl.transitlog.hfp.utils.DaemonThreadFactory
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class DWService(private val dataDirectory: Path, blobUploader: BlobUploader, privateBlobUploader: BlobUploader, msgAcknowledger: (MessageId) -> Unit) {
    private val log = KotlinLogging.logger {}

    //Count how many times we have tried to upload data but there was nothing to upload
    //If this value grows too high, print debug information
    private var noUploadCounter = 0

    private val fileWriterExecutorService = Executors.newCachedThreadPool(DaemonThreadFactory)
    private val scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)

    private val messageQueue = LinkedList<Pair<Hfp.Data, MessageId>>()

    private inline fun <R> useMessageQueue(func: () -> R) = synchronized(messageQueue, func)

    private val msgIds = mutableMapOf<Path, MutableList<MessageId>>()
    private val dwFiles = mutableMapOf<String, DWFile>()

    init {
        //Setup task for writing events to files
        scheduledExecutorService.scheduleWithFixedDelay({
            //Create copy of message queue and clear the queue
            val messages = useMessageQueue {
                val copy = messageQueue.toList()
                messageQueue.clear()
                return@useMessageQueue copy
            }

            log.info { "Writing ${messages.size} messages to CSV files" }

            val messagesByFile = messages.groupBy { (hfpData, _) -> getDWFile(hfpData) }
            //Write messages to files
            val futures = messagesByFile.map { (dwFile, messages) ->
                fileWriterExecutorService.submit {
                    messages.forEach { (hfpData, msgId) ->
                        val eventType = EventType.getEventType(hfpData.topic)
                        if (eventType == EventType.LightPriorityEvent) {
                            dwFile.writeEvent(LightPriorityEvent.parse(hfpData.topic, hfpData.payload))
                        } else {
                            dwFile.writeEvent(Event.parse(hfpData.topic, hfpData.payload))
                        }

                        val msgIdList = msgIds.computeIfAbsent(dwFile.path) { LinkedList<MessageId>() }
                        msgIdList.add(msgId)
                    }
                }
            }
            //Wait for writing to be done
            futures.forEach { it.get() }
        }, 15, 15, TimeUnit.SECONDS)

        //Setup task for uploading files to Azure every hour at 15min and 45min
        val now = ZonedDateTime.now()
        var initialUploadTime = now.withMinute(15)
        if (initialUploadTime.isBefore(now)) {
            initialUploadTime = initialUploadTime.plusMinutes(30)
            if (initialUploadTime.isBefore(now)) {
                initialUploadTime = initialUploadTime.plusMinutes(30)
            }
        }
        val initialDelay = now.until(initialUploadTime, ChronoUnit.SECONDS)

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
        }, initialDelay, Duration.ofMinutes(30).seconds, TimeUnit.SECONDS)
    }

    private fun getDWFile(hfpData: Hfp.Data): DWFile = dwFiles.computeIfAbsent(DWFile.createBlobName(hfpData)) { DWFile.createDWFile(hfpData, dataDirectory = dataDirectory) }

    fun addEvent(hfpData: Hfp.Data, msgId: MessageId) = useMessageQueue { messageQueue.add(hfpData to msgId) }
}