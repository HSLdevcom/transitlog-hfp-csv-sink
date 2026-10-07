package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp.Data
import fi.hsl.transitlog.hfp.domain.Event
import fi.hsl.transitlog.hfp.domain.EventType
import fi.hsl.transitlog.hfp.domain.IEvent
import fi.hsl.transitlog.hfp.domain.LightPriorityEvent
import fi.hsl.transitlog.hfp.utils.DaemonThreadFactory
import fi.hsl.transitlog.hfp.utils.DiskMessageIdJournal
import fi.hsl.transitlog.hfp.validator.EventValidator
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.ZonedDateTime
import java.util.ArrayDeque
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.math.min
import kotlin.math.roundToInt
import kotlin.time.ExperimentalTime
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId

@ExperimentalTime
class DWService(
    private val dataDirectory: Path,
    compressionLevel: Int,
    private val uploadAfterNotModified: Duration,
    private val sink: CSVSink,
    private val privateSink: CSVSink,
    private val msgAcknowledger: (MessageId) -> CompletableFuture<Void>,
    validators: List<EventValidator> = emptyList()
) {
    companion object {
        private const val MAX_QUEUE_SIZE = 750_000
        private const val ACK_BATCH_SIZE = 1024
        private const val CSV_SUFFIX = ".csv.zst"
        private const val JOURNAL_SUFFIX = ".csv.zst.msgids"
    }

    private val log = KotlinLogging.logger {}
    private var noUploadCounter = 0

    private val fileWriterExecutorService =
        Executors.newFixedThreadPool(
            (Runtime.getRuntime().availableProcessors() * 1.5).roundToInt(),
            DaemonThreadFactory
        )
    private val scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(DaemonThreadFactory)
    private val messageQueue = LinkedBlockingQueue<Pair<IEvent, MessageId>>(MAX_QUEUE_SIZE)
    private val deferredMessages = ArrayDeque<Pair<IEvent, MessageId>>()
    private val messageIdJournals = ConcurrentHashMap<Path, DiskMessageIdJournal>()
    private val failedFiles = ConcurrentHashMap.newKeySet<Path>()
    private val dwFiles = mutableMapOf<DWFile.FileFactory.BlobIdentifier, DWFile>()
    private val pendingUploads = mutableMapOf<Path, PendingUpload>()
    private val fileFactory =
        DWFile.FileFactory(dataDirectory, compressionLevel, uploadAfterNotModified, validators)

    private data class PendingUpload(
        val path: Path,
        val blobName: String,
        val private: Boolean,
        val metadata: Map<String, String>,
        val journalPath: Path,
        val messageCount: Int
    )

    private inner class DWFileWriterRunnable(
        private val dwFile: DWFile,
        private val messages: List<Pair<IEvent, MessageId>>
    ) : Runnable {
        override fun run() {
            try {
                val journal =
                    messageIdJournals.computeIfAbsent(dwFile.path) {
                        DiskMessageIdJournal.create(journalPath(dwFile.path))
                    }

                log.debug { "Writing ${messages.size} rows to ${dwFile.path}" }
                messages.forEach { (event, msgId) ->
                    dwFile.writeEvent(event)
                    journal.appendId(msgId.toByteArray())
                }
            } catch (e: Exception) {
                failedFiles.add(dwFile.path)
                throw e
            }
        }
    }

    init {
        Files.createDirectories(dataDirectory)
        cleanStaleFiles()

        scheduledExecutorService.scheduleWithFixedDelay(
            {
                val messages =
                    ArrayList<Pair<IEvent, MessageId>>(min(MAX_QUEUE_SIZE, messageQueue.size))
                while (messages.size < MAX_QUEUE_SIZE && deferredMessages.isNotEmpty()) {
                    messages += deferredMessages.removeFirst()
                }
                while (messages.size < MAX_QUEUE_SIZE) {
                    val msg = messageQueue.poll() ?: break
                    messages += msg
                }

                log.debug { "Writing ${messages.size} messages to CSV files" }
                val messagesByFile = mutableMapOf<DWFile, MutableList<Pair<IEvent, MessageId>>>()
                messages.forEach { message ->
                    val identifier = fileFactory.createBlobIdentifier(message.first)
                    val path = dataDirectory.resolve(identifier.blobName)
                    if (path in pendingUploads) {
                        deferredMessages.addLast(message)
                    } else {
                        val dwFile = getDWFile(identifier)
                        messagesByFile.getOrPut(dwFile) { mutableListOf() }.add(message)
                    }
                }
                val completionService = ExecutorCompletionService<Void>(fileWriterExecutorService)
                messagesByFile.forEach { (dwFile, fileMessages) ->
                    completionService.submit(DWFileWriterRunnable(dwFile, fileMessages), null)
                }

                var writeFailure: Throwable? = null
                repeat(messagesByFile.size) {
                    try {
                        completionService.take().get()
                    } catch (e: ExecutionException) {
                        val cause = e.cause ?: e
                        log.error(cause) { "Failed to write CSV file" }
                        if (writeFailure == null) {
                            writeFailure = cause
                        }
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw IllegalStateException("Interrupted while writing CSV files", e)
                    }
                }
                log.info { "Wrote ${messages.size} messages to CSV files" }
                writeFailure?.let {
                    throw IllegalStateException("Failed to write messages to CSV files", it)
                }
            },
            15,
            15,
            TimeUnit.SECONDS
        )

        val timeBetweenUploads = uploadAfterNotModified
        val initialDelay = getInitialDelayForUpload(timeBetweenUploads)
        scheduledExecutorService.scheduleAtFixedRate(
            {
                log.info { "Uploading files to blob storage" }
                var filesUploaded = 0

                for (dwFile in dwFiles.values.toList()) {
                    if (dwFile.isReadyForUpload() && dwFile.path !in failedFiles) {
                        try {
                            dwFile.close()
                            val journal =
                                checkNotNull(messageIdJournals[dwFile.path]) {
                                    "No message ID journal exists for ${dwFile.path}; refusing to upload"
                                }
                            journal.close()
                            forceFile(dwFile.path)

                            val pending =
                                PendingUpload(
                                    path = dwFile.path,
                                    blobName = dwFile.blobName,
                                    private = dwFile.private,
                                    metadata = dwFile.getMetadata(),
                                    journalPath = journalPath(dwFile.path),
                                    messageCount = journal.size
                                )
                            pendingUploads[dwFile.path] = pending
                        } catch (e: Exception) {
                            log.error(e) { "Failed to seal file ${dwFile.path} for upload" }
                        }
                    }
                }

                for (pending in pendingUploads.values.toList()) {
                    try {
                        val journal = DiskMessageIdJournal.openForReading(pending.journalPath)
                        check(
                            journal.forEachId { bytes -> MessageId.fromByteArray(bytes) } ==
                                pending.messageCount
                        ) {
                            "Message ID journal count does not match sealed metadata for ${pending.path}"
                        }

                        (if (pending.private) privateSink else sink)
                            .upload(pending.path, pending.blobName, pending.metadata)

                        log.info {
                            "Acknowledging ${pending.messageCount} messages written to ${pending.path}"
                        }
                        val acknowledgementBatch =
                            ArrayList<CompletableFuture<Void>>(ACK_BATCH_SIZE)
                        journal.forEachId { bytes ->
                            acknowledgementBatch.add(
                                msgAcknowledger(MessageId.fromByteArray(bytes))
                            )
                            if (acknowledgementBatch.size == ACK_BATCH_SIZE) {
                                awaitAcknowledgements(acknowledgementBatch)
                            }
                        }
                        awaitAcknowledgements(acknowledgementBatch)

                        pendingUploads.remove(pending.path)
                        dwFiles.entries.removeIf { it.value.path == pending.path }
                        messageIdJournals.remove(pending.path)
                        failedFiles.remove(pending.path)
                        Files.deleteIfExists(pending.path)
                        Files.deleteIfExists(pending.journalPath)
                        filesUploaded++
                    } catch (e: Exception) {
                        log.error(e) { "Failed to upload or acknowledge file ${pending.path}" }
                    }
                }

                if (filesUploaded == 0) {
                    noUploadCounter++
                } else {
                    noUploadCounter = 0
                }

                if (noUploadCounter > 2) {
                    val pendingFiles =
                        pendingUploads.values.joinToString("\n") { "${it.path} (${it.blobName})" }
                    val openFiles =
                        dwFiles.values.joinToString("\n") {
                            "${it.path} (${it.blobName}), last modified ${it.getLastModifiedAgo().toMinutes()}min ago"
                        }
                    log.warn {
                        "No files have been uploaded in last 2 tries, pending files:\n$pendingFiles\n$openFiles"
                    }
                }

                log.info { "Done uploading files to blob storage" }
            },
            initialDelay.toMillis(),
            timeBetweenUploads.toMillis(),
            TimeUnit.MILLISECONDS
        )
    }

    private fun cleanStaleFiles() {
        val existingFiles = Files.list(dataDirectory).use { it.toList() }
        for (path in existingFiles) {
            val name = path.fileName.toString()
            when {
                name.endsWith(CSV_SUFFIX) -> {
                    Files.deleteIfExists(path)
                    Files.deleteIfExists(journalPath(path))
                    log.warn {
                        "Discarded local file $path on startup; unacknowledged Pulsar messages will be replayed"
                    }
                }
                name.endsWith(JOURNAL_SUFFIX) -> {
                    Files.deleteIfExists(path)
                }
            }
        }
    }

    private fun getInitialDelayForUpload(interval: Duration): Duration {
        require(!interval.isZero && !interval.isNegative) {
            "Upload interval must be positive"
        }

        val now = ZonedDateTime.now()
        val startOfHour = now.withMinute(0).withSecond(0).withNano(0)
        val elapsedSinceHour = Duration.between(startOfHour, now)
        val completedIntervals = elapsedSinceHour.toNanos() / interval.toNanos()
        var nextUploadTime = startOfHour.plusNanos((completedIntervals + 1) * interval.toNanos())

        val startOfNextHour = startOfHour.plusHours(1)
        if (nextUploadTime.isAfter(startOfNextHour)) {
            nextUploadTime = startOfNextHour
        }
        return Duration.between(now, nextUploadTime)
    }

    private fun getDWFile(
        identifier: DWFile.FileFactory.BlobIdentifier
    ): DWFile =
        dwFiles.computeIfAbsent(identifier) {
            fileFactory.createDWFile(it)
        }

    private fun journalPath(csvPath: Path): Path =
        csvPath.resolveSibling(csvPath.fileName.toString() + ".msgids")

    private fun forceFile(path: Path) {
        FileChannel.open(path, StandardOpenOption.WRITE).use { it.force(true) }
    }

    private fun awaitAcknowledgements(batch: MutableList<CompletableFuture<Void>>) {
        if (batch.isNotEmpty()) {
            CompletableFuture.allOf(*batch.toTypedArray()).join()
            batch.clear()
        }
    }

    fun addEvent(hfpData: Data, msgId: MessageId) {
        val eventType = EventType.getEventType(hfpData.topic)
        val event =
            if (eventType == EventType.LightPriorityEvent) {
                safeParseLightPriorityEvent(hfpData)
            } else {
                safeParseEvent(hfpData)
            }

        if (event != null) {
            messageQueue.put(event to msgId)
        } else {
            msgAcknowledger(msgId)
        }
    }

    private fun safeParseEvent(hfpData: Data): Event? {
        return try {
            Event.parse(hfpData.topic, hfpData.payload)
        } catch (e: Exception) {
            log.warn { "Failed to parse Event: $e" }
            null
        }
    }

    private fun safeParseLightPriorityEvent(hfpData: Data): LightPriorityEvent? {
        return try {
            LightPriorityEvent.parse(hfpData.topic, hfpData.payload)
        } catch (e: Exception) {
            log.warn { "Failed to parse LightPriorityEvent: $e" }
            null
        }
    }
}
