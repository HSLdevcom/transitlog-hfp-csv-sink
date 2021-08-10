package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitlog.hfp.azure.BlobUploader
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId

class MessageHandler(private val pulsarApplicationContext: PulsarApplicationContext) : IMessageHandler {
    private val log = KotlinLogging.logger {}

    private val dwService = DWService(
        BlobUploader("", ""),
        BlobUploader("", ""),
        ::ack
    )

    private fun ack(messageId: MessageId) {
        pulsarApplicationContext.consumer!!.acknowledgeAsync(messageId)
            .exceptionally { throwable ->
                //TODO: should we stop the application when ack fails?
                log.error("Failed to ack Pulsar message", throwable)
                null
            }
            .thenRun {}
    }

    override fun handleMessage(msg: Message<Any>) {
        if (TransitdataSchema.hasProtobufSchema(msg, TransitdataProperties.ProtobufSchema.HfpData)) {
            try {
                val hfpData = Hfp.Data.parseFrom(msg.data)

                dwService.addEvent(hfpData, msg.messageId)
            } catch (e: Exception) {
                log.warn(e) { "Failed to handle message" }
            }
        } else {
            log.warn {
                "Received invalid protobuf schema, expected HfpData but received ${TransitdataSchema.parseFromPulsarMessage(msg).orElse(null)}"
            }
        }
    }
}