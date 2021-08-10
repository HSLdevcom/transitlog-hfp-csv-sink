package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.azure.BlobUploader
import fi.hsl.transitlog.hfp.domain.Event
import fi.hsl.transitlog.hfp.domain.EventType
import fi.hsl.transitlog.hfp.domain.LightPriorityEvent
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

    private val msgIds = mutableMapOf<Path, MutableList<MessageId>>()
    private val dwFiles = mutableMapOf<String, DWFile>()

    init {
        //Setup task for uploading files to Azure every hour at 45min
        val now = ZonedDateTime.now()
        val initialDelay = now.until(now.plusHours(1).withMinute(45), ChronoUnit.SECONDS)

        Executors.newSingleThreadScheduledExecutor { runnable ->
            val thread = Thread(runnable)
            thread.isDaemon = true
            thread.name = "DWUploadThread"
            return@newSingleThreadScheduledExecutor thread
        }
        .scheduleAtFixedRate({
            val dwFilesCopy = dwFiles.toMap()
            for ((key: String, dwFile: DWFile) in dwFilesCopy.entries) {
                if (dwFile.isReadyForUpload()) {
                    try {
                        //Close file for writing
                        dwFile.close()

                        //Upload file to Blob Storage
                        (if (dwFile.private) { privateBlobUploader } else { blobUploader }).uploadFromFile(dwFile.path)
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
        }, initialDelay, Duration.ofHours(1).seconds, TimeUnit.SECONDS)
    }

    private fun getDWFile(hfpData: Hfp.Data): DWFile = dwFiles.computeIfAbsent(DWFile.createBlobName(hfpData)) { DWFile.createDWFile(hfpData) }

    fun addEvent(hfpData: Hfp.Data, msgId: MessageId) {
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
}