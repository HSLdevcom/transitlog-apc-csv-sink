package fi.hsl.transitlog.parquet

import fi.hsl.common.passengercount.proto.PassengerCount
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertTrue

class PassengerCountParquetWriterBuilderTest {
    @field:TempDir
    lateinit var tempDir: Path
}