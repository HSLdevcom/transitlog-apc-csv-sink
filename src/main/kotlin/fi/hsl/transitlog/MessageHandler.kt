package fi.hsl.transitlog

import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.common.pulsar.IMessageHandler
import fi.hsl.common.pulsar.PulsarApplicationContext
import fi.hsl.common.transitdata.TransitdataProperties
import fi.hsl.common.transitdata.TransitdataSchema
import fi.hsl.transitlog.sink.azure.AzureSink
import fi.hsl.transitlog.sink.azure.BlobUploader
import mu.KotlinLogging
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Paths

private val log = KotlinLogging.logger {}

class MessageHandler(private val pulsarApplicationContext: PulsarApplicationContext) : IMessageHandler, AutoCloseable {
    private val config = pulsarApplicationContext.config!!

    private val blobConnectionString = config.getString("application.sink.azure.blobConnectionString")
    private val blobContainer = config.getString("application.sink.azure.blobContainer")

    private val apcArchiveService = ApcArchiveService(Paths.get("apc"), AzureSink(BlobUploader(blobConnectionString, blobContainer)), ::ack)

    private fun ack(messageId: MessageId) {
        pulsarApplicationContext.consumer!!.acknowledgeAsync(messageId)
            .exceptionally { throwable ->
                //TODO: should we stop the application when ack fails?
                log.error("Failed to ack Pulsar message", throwable)
                null
            }
    }

    override fun handleMessage(msg: Message<*>) {
        if (TransitdataSchema.hasProtobufSchema(msg, TransitdataProperties.ProtobufSchema.PassengerCount)) {
            try {
                val apcData = PassengerCount.Data.parseFrom(msg.data)

                apcArchiveService.addToWriteQueue(apcData, msg.messageId)
            } catch (e: Exception) {
                log.warn(e) { "Failed to handle message" }

                //Ack messages that could not be handled
                ack(msg.messageId)
            }
        } else {
            log.warn {
                "Received invalid protobuf schema, expected PassengerCount but received ${TransitdataSchema.parseFromPulsarMessage(msg).orElse(null)}"
            }
        }
    }

    override fun close() {
        apcArchiveService.close()
    }
}