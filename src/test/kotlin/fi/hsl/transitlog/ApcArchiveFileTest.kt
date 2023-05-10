package fi.hsl.transitlog

import org.junit.jupiter.api.io.TempDir
import java.nio.file.Path
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

class ApcArchiveFileTest {
    @field:TempDir
    lateinit var tempDir: Path

    @Test
    fun `Test ApcArchiveFileDescriptorFactory creates descriptor with correct start and end times`() {
        val apcFileDescriptorFactory = ApcArchiveFile.ApcFileDescriptorFactory(tempDir, Duration.ofMinutes(15))
        
        val timestamp = ZonedDateTime.of(2025, 1, 1, 12, 37, 0, 0, ZoneId.of("Europe/Helsinki")).toInstant()

        val apcFileDescriptor = apcFileDescriptorFactory.createApcFileDescriptor(timestamp.toEpochMilli())

        assertEquals(
            ZonedDateTime.of(2025, 1, 1, 12, 30, 0, 0, ZoneId.of("Europe/Helsinki")).toInstant(),
            apcFileDescriptor.contentStart
        )
        assertEquals(
            ZonedDateTime.of(2025, 1, 1, 12, 45, 0, 0, ZoneId.of("Europe/Helsinki")).toInstant(),
            apcFileDescriptor.contentEnd
        )
    }
}