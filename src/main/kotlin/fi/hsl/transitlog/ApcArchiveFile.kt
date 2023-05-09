package fi.hsl.transitlog

import fi.hsl.common.passengercount.proto.PassengerCount
import fi.hsl.transitlog.parquet.PassengerCountParquetWriterBuilder
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * @param contentDuration The period for which the file should contain data
 */
class ApcArchiveFile(val apcFileDescriptor: ApcFileDescriptorFactory.ApcFileDescriptor, private val fastUpload: Boolean) : AutoCloseable {
    //Path to the file
    val path = apcFileDescriptor.file
    //Duration for which the file should contain data
    private val contentDuration = Duration.ofNanos(apcFileDescriptor.contentStart.until(apcFileDescriptor.contentEnd, ChronoUnit.NANOS))

    //Path to CRC file created by Parquet writer
    private val crcPath = path.parent.resolve(".${path.fileName}.crc")

    private val passengerCountParquetWriter = PassengerCountParquetWriterBuilder(path)
        .withCompressionCodec(CompressionCodecName.ZSTD) //Zstd has best balance between compression speed and ratio. Azure Data Factory might not support Zstd
        .build()

    private val lastModified = AtomicLong(System.nanoTime())

    private var closed = false

    private var rowCount = 0
    private var minTst: Instant? = null
    private var maxTst: Instant? = null

    private var maxReceivedAt: Instant? = null

    fun isReadyForUpload(): Boolean {
        val lastModifiedAgo = Duration.ofNanos(System.nanoTime() - lastModified.get())

        //Fast upload is possible if the file contains data for the last 30 seconds and it has not been modified for one minute
        val fastUploadPossible = fastUpload
                && maxReceivedAt != null
                && maxReceivedAt!!.until(apcFileDescriptor.contentEnd, ChronoUnit.SECONDS) <= 30
                && lastModifiedAgo > Duration.ofMinutes(1)

        return fastUploadPossible || lastModifiedAgo > contentDuration
    }

    fun writeApc(passengerCount: PassengerCount.Data) {
        if (closed) {
            throw IllegalStateException("Writer already closed")
        }

        passengerCountParquetWriter.write(passengerCount)

        rowCount++

        val tstAsInstant = Instant.ofEpochMilli(passengerCount.payload.tst)
        if (minTst == null || tstAsInstant < minTst) {
            minTst = tstAsInstant
        }
        if (maxTst == null || tstAsInstant > maxTst) {
            maxTst = tstAsInstant
        }

        val receivedAtAsInstant = Instant.ofEpochMilli(passengerCount.receivedAt)
        if (maxReceivedAt == null || receivedAtAsInstant > maxReceivedAt) {
            maxReceivedAt = receivedAtAsInstant
        }

        lastModified.set(System.nanoTime())
    }

    /**
     * Tags that can be used for querying blobs
     */
    fun getTags(): Map<String, String> {
        val tags = mutableMapOf<String, String>()

        if (minTst != null) {
            tags["min_tst"] = DateTimeFormatter.ISO_INSTANT.format(minTst)
        }
        if (maxTst != null) {
            tags["max_tst"] = DateTimeFormatter.ISO_INSTANT.format(maxTst)
        }

        return tags.toMap()
    }

    /**
     * Metadata that is not useful for querying blobs
     */
    fun getMetadata(): Map<String, String> {
        val metadata = mutableMapOf<String, String>()

        metadata["row_count"] = rowCount.toString()
        metadata["parquet_crc"] = Base64.getEncoder().encodeToString(Files.readAllBytes(crcPath))

        return metadata.toMap()
    }

    fun delete() {
        Files.deleteIfExists(path)
        Files.deleteIfExists(crcPath)
    }

    override fun close() {
        if (!closed) {
            passengerCountParquetWriter.close()
        }
        closed = true
    }

    class ApcFileDescriptorFactory(private val dataDirectory: Path, private val contentDuration: Duration) {
        companion object {
            private val DATE_HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")
        }

        data class ApcFileDescriptor(val file: Path, val contentStart: Instant, val contentEnd: Instant)

        /**
         * @param timestamp Timestamp in milliseconds
         *
         * @return File descriptor
         */
        fun createApcFileDescriptor(timestamp: Long): ApcFileDescriptor {
            val receivedAtUtc = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)

            val receivedAtUtcTruncated = receivedAtUtc.truncatedTo(ChronoUnit.HOURS)

            val dateHour = receivedAtUtc.toLocalDateTime()
            val part = (receivedAtUtcTruncated.until(receivedAtUtc, ChronoUnit.NANOS) / contentDuration.toNanos()) + 1

            val contentStart = receivedAtUtcTruncated.toInstant().plus(contentDuration.multipliedBy(part - 1))
            val contentEnd = contentStart.plus(contentDuration)

            return ApcFileDescriptor(dataDirectory.resolve("apc_${DATE_HOUR_FORMATTER.format(dateHour)}-$part.parquet"), contentStart, contentEnd)
        }
    }
}