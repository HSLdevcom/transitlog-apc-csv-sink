package fi.hsl.transitlog

import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.transitlog.sink.Sink
import mu.KotlinLogging
import org.apache.pulsar.client.api.MessageId
import java.nio.file.Path
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.math.min

private val log = KotlinLogging.logger {}

class ApcArchiveService(private val dataDirectory: Path, private val sink: Sink, private val ack: (MessageId) -> Unit) : AutoCloseable {
    companion object {
        private const val MAX_QUEUE_SIZE = 10000

        private val BATCH_WRITE_INTERVAL = Duration.ofSeconds(30)

        //Each file contains data for 15 minutes
        //TODO: this should be configurable
        private val CONTENT_DURATION = Duration.ofMinutes(15)
    }

    private val scheduledExecutor = Executors.newSingleThreadScheduledExecutor {
        val thread = Thread(it)
        thread.name = "AppArchiveServiceThread"
        thread.isDaemon = true

        return@newSingleThreadScheduledExecutor thread
    }

    private val messageQueue = LinkedBlockingQueue<Pair<PassengerCount.Data, MessageId>>(MAX_QUEUE_SIZE)

    private val files = mutableMapOf<Path, ApcArchiveFile>()
    private val msgIdsByFile = mutableMapOf<ApcArchiveFile, MutableList<MessageId>>()

    init {
        //Start task for writing data to files in batches every 30s
        scheduledExecutor.scheduleWithFixedDelay(::writeData, BATCH_WRITE_INTERVAL.toMillis(), BATCH_WRITE_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)

        //Start task for uploading data to Azure Blob Storage
        scheduledExecutor.scheduleWithFixedDelay(::uploadReadyFiles, (CONTENT_DURATION + BATCH_WRITE_INTERVAL).toMillis(), CONTENT_DURATION.dividedBy(3).toMillis(), TimeUnit.MILLISECONDS)
    }

    private fun writeData() {
        //Poll up to MAX_QUEUE_SIZE events from queue
        val messages = ArrayList<Pair<PassengerCount.Data, MessageId>>(min(MAX_QUEUE_SIZE, messageQueue.size))
        for (i in 1..MAX_QUEUE_SIZE) {
            val msg = messageQueue.poll()
            if (msg == null) {
                break
            } else {
                messages += msg
            }
        }

        log.debug { "Writing ${messages.size} messages to files" }

        val dataByFile = messages
            //Group data to files based on the hour it was _received_
            .groupBy { ApcArchiveFile.createApcFileName(it.first.receivedAt) }
            .mapKeys { dataDirectory.resolve(it.key) }
            .mapKeys {
                files.computeIfAbsent(it.key) { path ->
                    ApcArchiveFile(path, CONTENT_DURATION)
                }
            }

        dataByFile.forEach { (file, data) ->
            log.info { "Writing ${data.size} APC messages to ${file.path}" }

            data.forEach { (apcData, messageId) ->
                file.writeApc(apcData)

                msgIdsByFile.computeIfAbsent(file) { LinkedList<MessageId>() }.add(messageId)
            }
        }
    }

    private fun uploadReadyFiles() {
        files.values
            .filter { it.isReadyForUpload() }
            .forEach { apcArchiveFile ->
                apcArchiveFile.close()

                val messageIds = msgIdsByFile[apcArchiveFile] ?: emptyList()

                log.info { "Uploading ${apcArchiveFile.path}" }
                sink.upload(apcArchiveFile.path, apcArchiveFile.path.fileName.toString(), metadata = apcArchiveFile.getMetadata(), tags = apcArchiveFile.getTags())
                log.info { "Uploaded ${apcArchiveFile.path}, acknowledging ${messageIds.size} messages" }

                messageIds.forEach(ack)

                apcArchiveFile.delete()

                files.remove(apcArchiveFile.path)
                msgIdsByFile.remove(apcArchiveFile)
            }
    }

    fun addToWriteQueue(passengerCount: PassengerCount.Data, messageId: MessageId) {
        messageQueue.put(passengerCount to messageId)
    }

    override fun close() {
        scheduledExecutor.shutdownNow()
        scheduledExecutor.awaitTermination(5, TimeUnit.SECONDS)
    }
}