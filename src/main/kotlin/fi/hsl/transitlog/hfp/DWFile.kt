package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.domain.EventType
import fi.hsl.transitlog.hfp.domain.IEvent
import fi.hsl.transitlog.hfp.utils.Deduplicator
import fi.hsl.transitlog.hfp.validator.EventValidator
import mu.KotlinLogging
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.BufferedOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.*
import java.time.format.DateTimeFormatter
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties

private val log = KotlinLogging.logger {}

class DWFile private constructor(val path: Path, val private: Boolean, val invalid: Boolean, val blobName: String, private val csvHeader: List<String>, private val eventType: Hfp.Topic.EventType, compressionLevel: Int) : AutoCloseable {
    companion object {
        private const val WRITE_BUFFER_SIZE = 32768 //32KiB
    }

    private val csvPrinter = CSVPrinter(
        OutputStreamWriter(ZstdCompressorOutputStream(BufferedOutputStream(Files.newOutputStream(path), WRITE_BUFFER_SIZE), compressionLevel), StandardCharsets.UTF_8),
        CSVFormat.RFC4180.withHeader(*csvHeader.toTypedArray())
    )
    private var open: Boolean = true
    private var lastModified: Long = System.nanoTime()

    private var rowCount = 0
    private var minTst: OffsetDateTime? = null
    private var maxTst: OffsetDateTime? = null
    private var minOday: LocalDate? = null
    private var maxOday: LocalDate? = null

    //Assumes that events are the same if event type, timestamp and unique vehicle ID are equal
    private val deduplicator = Deduplicator<IEvent, Long>(if (eventType == Hfp.Topic.EventType.VP) { 250_000 } else { 1000 }) { ievent ->
        val bytes = (ievent.eventType?.toByteArray(StandardCharsets.UTF_8) ?: byteArrayOf()) + ievent.tst.toInstant().toEpochMilli().toBigInteger().toByteArray() + (ievent.uniqueVehicleId?.toByteArray(StandardCharsets.UTF_8) ?: byteArrayOf())
        return@Deduplicator MurmurHash3.hash128x64(bytes)[0]
    }

    fun <E : IEvent> writeEvent(event: E) {
        if (!open) {
            throw IllegalStateException("File has been closed for writing")
        }

        deduplicator.consumeOnlyOnce(event) { event ->
            val properties = event::class.declaredMemberProperties.sortedBy { it.name }
            val values = properties.map { (it as KProperty1<Any, Any?>).get(event)?.toString() ?: "" }

            if (values.size != csvHeader.size) {
                log.warn {
                    "CSV record has different amount of values than CSV header. Record: '${values.joinToString(",")}', header: '${
                        csvHeader.joinToString(
                            ","
                        )
                    }'"
                }
            }

            csvPrinter.printRecord(values)

            rowCount++
            if (minTst == null || event.tst < minTst) {
                minTst = event.tst
            }
            if (maxTst == null || event.tst > maxTst) {
                maxTst = event.tst
            }
            if (minOday == null || (event.oday != null && event.oday!! < minOday)) {
                minOday = event.oday
            }
            if (maxOday == null || (event.oday != null && event.oday!! > maxOday)) {
                maxOday = event.oday
            }
        }
        
        lastModified = System.nanoTime()
    }

    fun getLastModifiedAgo(): Duration = Duration.ofNanos(System.nanoTime() - lastModified)

    //File is ready for uploading if it has not been modified for 15 minutes
    //(files are created based on the time that the HFP message was _received_ and we assume that are not long gaps between messages)
    //TODO: this should be configurable
    fun isReadyForUpload(): Boolean = getLastModifiedAgo() > Duration.ofMinutes(15)

    /**
     * Returns metadata about the file contents. Can be used as blob metadata
     */
    fun getMetadata(): Map<String, String> {
        val metadata = mutableMapOf(
            "row_count" to rowCount.toString(),
            "eventType" to eventType.toString()
        )

        metadata["invalid"] = invalid.toString()

        if (minTst != null) {
            metadata["min_tst"] = minTst!!.format(DateTimeFormatter.ISO_INSTANT)
        }
        if (maxTst != null) {
            metadata["max_tst"] = maxTst!!.format(DateTimeFormatter.ISO_INSTANT)
        }
        if (minOday != null) {
            metadata["min_oday"] = minOday!!.format(DateTimeFormatter.ISO_LOCAL_DATE)
        }
        if (maxOday != null) {
            metadata["max_oday"] = maxOday!!.format(DateTimeFormatter.ISO_LOCAL_DATE)
        }

        return metadata.toMap()
    }

    /**
     * Closes file for writing. After this function has been invoked, writeEvent cannot be used
     */
    override fun close() {
        //Avoid IOException if trying to close the file more than once
        if (open) {
            csvPrinter.close(true)
        }
        open = false
    }

    /**
     * Factory for creating DWFiles
     *
     * @param timezone Timezone that is used in the file name
     */
    class FileFactory(private val dataDirectory: Path, private val compressionLevel: Int, private val validators: List<EventValidator> = emptyList()) {
        companion object {
            private val DATE_HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")

            //Events that contain private data and that should be always uploaded to private blob container
            private val PRIVATE_EVENTS = setOf("DA", "DOUT", "BA", "BOUT")
        }

        data class BlobIdentifier(val baseName: String, val eventType: EventType, val hfpEventType: Hfp.Topic.EventType, val private: Boolean, val invalid: Boolean) {
            val blobName = "${baseName}_utc_${hfpEventType}${if (private) { "_private" } else { "" }}${if (invalid) { "_invalid" } else { "" }}.csv.zst"
        }

        private fun isValidEvent(event: IEvent): Boolean = validators.all { it.isValidEvent(event) }

        private fun isPrivate(event: IEvent): Boolean {
            return event.journeyType != "journey" || event.eventType in PRIVATE_EVENTS
        }

        fun createBlobIdentifier(event: IEvent): BlobIdentifier {
            //Use MQTT received timestamp for file names (messages can get delayed and relying on tst timestamp could cause files to be overwritten)
            val localDateTime = event.receivedAt!!.atOffset(ZoneOffset.UTC).toLocalDateTime()
            val timestampFormatted = localDateTime.format(DATE_HOUR_FORMATTER)

            //Create files that contain 15min data (1 -> data for minutes 0-14, 2 -> data for minutes 15-29 etc.)
            val minuteNumber = 1 + (localDateTime.minute / 15)

            val baseName = "$timestampFormatted-$minuteNumber"

            val private = isPrivate(event)
            val invalid = !isValidEvent(event)

            val hfpEventType = Hfp.Topic.EventType.valueOf(event.eventType!!)

            //Use OtherEvent as default in case new event types are added
            val eventType = EventType.getEventType(Hfp.Topic.JourneyType.valueOf(event.journeyType!!), hfpEventType)
                ?: EventType.OtherEvent

            return BlobIdentifier(baseName, eventType, hfpEventType, private, invalid)
        }

        /**
         * Creates a DWFile based on the identifier. The same DWFile should be reused for writing data with same identifier i.e. DWFile instances should be stored to a map etc.
         */
        fun createDWFile(blobIdentifier: BlobIdentifier): DWFile {
            val path = dataDirectory.resolve(blobIdentifier.blobName)

            return DWFile(path, blobIdentifier.private, blobIdentifier.invalid, blobIdentifier.blobName, blobIdentifier.eventType.csvHeader, blobIdentifier.hfpEventType, compressionLevel)
        }
    }
}