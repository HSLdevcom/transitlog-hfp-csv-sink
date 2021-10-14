package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.domain.EventType
import fi.hsl.transitlog.hfp.domain.IEvent
import fi.hsl.transitlog.hfp.utils.Deduplicator
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

class DWFile private constructor(val path: Path, val private: Boolean, val blobName: String, private val csvHeader: List<String>, private val eventType: Hfp.Topic.EventType) : AutoCloseable {
    companion object {
        private val HFP_TIMEZONE = ZoneId.of("Europe/Helsinki")
        private val DATE_HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")

        //Events that contain private data and that should be always uploaded to private blob container
        private val PRIVATE_EVENTS = setOf(Hfp.Topic.EventType.DA, Hfp.Topic.EventType.DOUT, Hfp.Topic.EventType.BA, Hfp.Topic.EventType.BOUT)

        fun createBlobName(hfpData: Hfp.Data): String {
            //Append "_private" to files that contain private data
            val private = if (hfpData.topic.isPrivateData()) {
                "_private"
            } else {
                ""
            }

            //Use MQTT received timestamp for file names (messages can get delayed and relying on tst timestamp could cause files to be overwritten)
            val timestamp = Instant.ofEpochMilli(hfpData.topic.receivedAt).atZone(HFP_TIMEZONE).toLocalDateTime().format(DATE_HOUR_FORMATTER)

            return "${timestamp}_${hfpData.topic.eventType}${private}.csv.zst"
        }

        private fun Hfp.Topic.isPrivateData(): Boolean {
            //If the journey type is not "journey", the data is always private
            if (journeyType != Hfp.Topic.JourneyType.journey) {
                return true
            }

            //Otherwise, check if the event type is private
            return eventType in PRIVATE_EVENTS
        }

        /**
         * @param dataDirectory Directory where the file will be stored
         */
        fun createDWFile(hfpData: Hfp.Data, dataDirectory: Path = Files.createTempDirectory("hfp")): DWFile {
            val blobName = createBlobName(hfpData)

            val path = dataDirectory.resolve(blobName)
            Files.createDirectories(path.parent)

            //Use OtherEvent as default in case new event types are added
            val eventType = EventType.getEventType(hfpData.topic) ?: EventType.OtherEvent

            return DWFile(path, hfpData.topic.isPrivateData(), blobName, eventType.csvHeader, hfpData.topic.eventType)
        }
    }

    private val log = KotlinLogging.logger {}

    private val csvPrinter = CSVPrinter(
        OutputStreamWriter(BufferedOutputStream(ZstdCompressorOutputStream(Files.newOutputStream(path)), 65536), StandardCharsets.UTF_8),
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
    private val deduplicator = Deduplicator<IEvent, Long> { ievent ->
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
            lastModified = System.nanoTime()

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
    }

    fun getLastModifiedAgo(): Duration = Duration.ofNanos(System.nanoTime() - lastModified)

    //File is ready for uploading if it has not been modified for 45 minutes (we assume that HFP data is not delayed by more than one hour)
    //TODO: this should be configurable
    fun isReadyForUpload(): Boolean = getLastModifiedAgo() > Duration.ofMinutes(45)

    /**
     * Returns metadata about the file contents. Can be used as blob metadata
     */
    fun getMetadata(): Map<String, String> {
        val metadata = mutableMapOf(
            "row_count" to rowCount.toString(),
            "eventType" to eventType.toString()
        )

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
}