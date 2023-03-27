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
class ApcArchiveFile(val path: Path, private val contentDuration: Duration) : AutoCloseable {
    companion object {
        private val DATE_HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH")

        /**
         * @param timestamp Timestamp in milliseconds
         *
         * @return File name
         */
        fun createApcFileName(timestamp: Long): String {
            val receivedAtLocalTime = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC).toLocalDateTime()

            val dateHour = receivedAtLocalTime.truncatedTo(ChronoUnit.HOURS)
            val minute = (receivedAtLocalTime.minute / 15) + 1

            return "apc_${DATE_HOUR_FORMATTER.format(dateHour)}-$minute.parquet"
        }
    }

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

    fun isReadyForUpload(): Boolean {
        return Duration.ofNanos(System.nanoTime() - lastModified.get()) > contentDuration
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
}