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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class DWService(blobUploader: BlobUploader, privateBlobUploader: BlobUploader, msgAcknowledger: (MessageId) -> Unit) {
    private val log = KotlinLogging.logger {}

    private val executorService = Executors.newScheduledThreadPool(1, DaemonThreadFactory)

    private val messageQueue = mutableListOf<Pair<Hfp.Data, MessageId>>()

    private inline fun <R> useMessageQueue(func: () -> R) = synchronized(messageQueue, func)

    private val msgIds = mutableMapOf<Path, MutableList<MessageId>>()
    private val dwFiles = mutableMapOf<String, DWFile>()

    init {
        //Setup task for writing events to files
        executorService.scheduleWithFixedDelay({
            //Create copy of message queue and clear the queue
            val messages = useMessageQueue {
                val copy = messageQueue.toList()
                messageQueue.clear()
                return@useMessageQueue copy
            }

            log.info { "Writing ${messages.size} messages to CSV files" }

            //Write messages to files
            messages.forEach { (hfpData, msgId) ->
                val dwFile = getDWFile(hfpData)

                val eventType = EventType.getEventType(hfpData.topic)
                if (eventType == EventType.LightPriorityEvent) {
                    dwFile.writeEvent(LightPriorityEvent.parse(hfpData.topic, hfpData.payload))
                } else {
                    dwFile.writeEvent(Event.parse(hfpData.topic, hfpData.payload))
                }

                val msgIdList = msgIds.computeIfAbsent(dwFile.path) { mutableListOf() }
                msgIdList.add(msgId)
            }
        }, 15, 15, TimeUnit.SECONDS)

        //Setup task for uploading files to Azure every hour at 45min
        val now = ZonedDateTime.now()
        val initialDelay = now.until(now.plusHours(1).withMinute(45), ChronoUnit.SECONDS)

        executorService.scheduleAtFixedRate({
            val dwFilesCopy = dwFiles.toMap()

            log.info { "Uploading files to blob storage" }

            for ((key: String, dwFile: DWFile) in dwFilesCopy.entries) {
                if (dwFile.isReadyForUpload()) {
                    try {
                        //Close file for writing
                        dwFile.close()

                        //Upload file to Blob Storage
                        (if (dwFile.private) { privateBlobUploader } else { blobUploader }).uploadFromFile(dwFile.path, blobName = dwFile.blobName)
                        //Acknowledge all messages that were in the file
                        msgIds[dwFile.path]!!.forEach(msgAcknowledger)

                        msgIds.remove(dwFile.path)
                        dwFiles.remove(key)

                        Files.delete(dwFile.path)
                    } catch (e: Exception) {
                        log.error(e) {
                            "Failed to upload file to Blob Storage"
                        }
                    }
                }
            }

            log.info { "Done uploading files to blob storage" }
        }, initialDelay, Duration.ofHours(1).seconds, TimeUnit.SECONDS)
    }

    private fun getDWFile(hfpData: Hfp.Data): DWFile = dwFiles.computeIfAbsent(DWFile.createBlobName(hfpData)) { DWFile.createDWFile(hfpData) }

    fun addEvent(hfpData: Hfp.Data, msgId: MessageId) = useMessageQueue { messageQueue.add(hfpData to msgId) }
}