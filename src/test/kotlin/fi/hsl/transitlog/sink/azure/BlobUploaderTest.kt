package fi.hsl.transitlog.sink.azure

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.assertEquals

@Testcontainers
class BlobUploaderTest {
    @TempDir
    lateinit var tempDir: Path

    @Container
    val azurite = GenericContainer(DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite"))
        .withCommand("azurite-blob --blobHost 0.0.0.0 --blobPort 10000")
        .withExposedPorts(10000)

    private lateinit var connString: String

    @BeforeEach
    fun generateConnString() {
        connString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://${azurite.host}:${azurite.firstMappedPort}/devstoreaccount1;"
    }

    @Test
    fun `Test uploading blobs`() {
        val testFile = tempDir.resolve("test.dat")

        val data = ByteArray(10 * 1024)
        ThreadLocalRandom.current().nextBytes(data)
        Files.write(testFile, data)

        val uploader = BlobUploader(connString, "test")
        val uploadedBlobName = uploader.uploadFromFile(testFile)

        assertEquals("test.dat", uploadedBlobName)
    }
}