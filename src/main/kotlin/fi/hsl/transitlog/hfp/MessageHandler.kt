package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitlog.hfp.azure.AzureSink
import fi.hsl.transitlog.hfp.azure.BlobUploader
import fi.hsl.transitlog.hfp.validator.EventValidator
import fi.hsl.transitlog.hfp.validator.OdayValidator
import fi.hsl.transitlog.hfp.validator.TimestampValidator
import fi.hsl.transitlog.hfp.utils.TestSink
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.ZoneId
import kotlin.time.ExperimentalTime

private val log = KotlinLogging.logger {}

@ExperimentalTime
class MessageHandler(private val pulsarApplicationContext: PulsarApplicationContext) : IMessageHandler {
    private val config = pulsarApplicationContext.config!!

    private val connectionString = config.getString("application.blobConnectionString")

    private val sinkType = pulsarApplicationContext.config!!.getString("application.sinkType")

    private val sink = if ("azure" == sinkType) {
        AzureSink(BlobUploader(connectionString, pulsarApplicationContext.config!!.getString("application.blobContainer")))
    } else {
        TestSink()
    }

    private val privateSink = if ("azure" == sinkType) {
        //Uploads to private container that is not accessible without authentication
        AzureSink(BlobUploader(connectionString, pulsarApplicationContext.config!!.getString("application.blobContainerPrivate")))
    } else {
        TestSink()
    }

    private val dataDirectory: Path = Paths.get("hfp").also {
        Files.createDirectories(it)
    }

    private val validators: List<EventValidator> = mutableListOf<EventValidator>().also {
        if (config.getBoolean("validator.tst.enabled")) {
            it.add(TimestampValidator(config.getDuration("validator.tst.maxPast"), config.getDuration("validator.tst.maxFuture")))
        }

        if (config.getBoolean("validator.oday.enabled")) {
            it.add(OdayValidator(ZoneId.of("Europe/Helsinki"), config.getInt("validator.oday.maxPast"), config.getInt("validator.oday.maxFuture")))
        }
    }

    private val dwService = DWService(
        dataDirectory,
        pulsarApplicationContext.config!!.getInt("application.zstdCompressionLevel"),
        sink,
        privateSink,
        ::ack,
        validators
    )

    private var lastHandledMessageTime = System.nanoTime()
    private var lastAcknowledgedMessageTime = System.nanoTime()

    fun getLastHandledMessageTime(): Long = lastHandledMessageTime

    fun getLastAcknowledgedMessageTime(): Long = lastAcknowledgedMessageTime

    private fun ack(messageId: MessageId) {
        pulsarApplicationContext.consumer!!.acknowledgeAsync(messageId)
            .exceptionally { throwable ->
                //TODO: should we stop the application when ack fails?
                log.error("Failed to ack Pulsar message", throwable)
                null
            }
            .thenRun { lastAcknowledgedMessageTime = System.nanoTime() }
    }

    override fun handleMessage(msg: Message<Any>) {
        if (TransitdataSchema.hasProtobufSchema(msg, TransitdataProperties.ProtobufSchema.HfpData)) {
            try {
                val hfpData = Hfp.Data.parseFrom(msg.data)

                dwService.addEvent(hfpData, msg.messageId)

                lastHandledMessageTime = System.nanoTime()
            } catch (e: Exception) {
                log.warn(e) { "Failed to handle message" }
                e.printStackTrace()
            }
        } else {
            log.warn {
                "Received invalid protobuf schema, expected HfpData but received ${TransitdataSchema.parseFromPulsarMessage(msg).orElse(null)}"
            }
        }
    }
}