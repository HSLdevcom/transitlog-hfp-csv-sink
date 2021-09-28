package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.domain.EventType
import mu.KotlinLogging
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import java.io.BufferedOutputStream
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import kotlin.reflect.KProperty1
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.jvmErasure

class DWFile private constructor(val path: Path, val private: Boolean, val blobName: String, private val csvHeader: List<String>) : AutoCloseable {
    companion object {
        private val HFP_TIMEZONE = ZoneId.of("Europe/Helsinki")
        private val DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")

        fun createBlobName(hfpData: Hfp.Data): String {
            //Use private directory for events during deadrun journeys
            val private = if (hfpData.topic.isPrivateData()) {
                "private/"
            } else {
                ""
            }

            //Use OtherEvent as default in case new event types are added
            val eventType = EventType.getEventType(hfpData.topic) ?: EventType.OtherEvent

            val timestamp = Instant.ofEpochSecond(hfpData.payload.tsi).atZone(HFP_TIMEZONE).toLocalDateTime().format(DATE_TIME_FORMATTER)

            return "csv/$private$eventType/$timestamp.csv.zst"
        }

        private fun Hfp.Topic.isPrivateData(): Boolean = journeyType != Hfp.Topic.JourneyType.journey

        fun createDWFile(hfpData: Hfp.Data): DWFile {
            //TODO: temp files will be located in /tmp which is usually located in memory, is this a problem?
            val path = Files.createTempFile("hfp", ".csv.zst")

            //Use OtherEvent as default in case new event types are added
            val eventType = EventType.getEventType(hfpData.topic) ?: EventType.OtherEvent

            return DWFile(path, hfpData.topic.isPrivateData(), createBlobName(hfpData), eventType.csvHeader)
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

    fun writeEvent(event: Any) {
        if (!open) {
            throw IllegalStateException("File has been closed for writing")
        }

        val properties = event::class.declaredMemberProperties.sortedBy { it.name }
        val values = properties.map { (it as KProperty1<Any, Any?>).get(event)?.toString() ?: "" }

        if (values.size != csvHeader.size) {
            log.warn { "CSV record has different amount of values than CSV header. Record: '${values.joinToString(",")}', header: '${csvHeader.joinToString(",")}'" }
        }

        csvPrinter.printRecord(values)
        lastModified = System.nanoTime()
        rowCount++

        //TODO: think about better way to do this...
        val maybeTstProperty = properties.find { it.name == "tst" && it.returnType.jvmErasure.java == OffsetDateTime::class.java }
        if (maybeTstProperty != null) {
            val tst = (maybeTstProperty as KProperty1<Any, OffsetDateTime>).get(event)
            if (minTst == null || tst < minTst) {
                minTst = tst
            }
            if (maxTst == null || tst > maxTst) {
                maxTst = tst
            }
        }
    }

    //File is ready for uploading if it has not been modified for 60 minutes (we assume that HFP data is not delayed by more than one hour)
    fun isReadyForUpload(): Boolean = Duration.ofNanos(System.nanoTime() - lastModified) > Duration.ofMinutes(60)

    /**
     * Returns metadata about the file contents. Can be used as blob metadata
     */
    fun getMetadata(): Map<String, String> {
        val metadata = mutableMapOf("row_count" to rowCount.toString())

        if (minTst != null) {
            metadata["min_tst"] = minTst!!.format(DateTimeFormatter.ISO_INSTANT)
        }
        if (maxTst != null) {
            metadata["min_tst"] = maxTst!!.format(DateTimeFormatter.ISO_INSTANT)
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