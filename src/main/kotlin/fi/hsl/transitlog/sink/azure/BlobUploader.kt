package fi.hsl.transitlog.sink.azure

import com.azure.storage.blob.BlobServiceClientBuilder
import mu.KotlinLogging
import java.io.BufferedOutputStream
import java.nio.file.Files
import java.nio.file.Path

private const val BUFFER_SIZE = 65536;

class BlobUploader(connectionString: String, blobContainer: String) {
    private val log = KotlinLogging.logger {}

    private val blobServiceClient = BlobServiceClientBuilder().connectionString(connectionString).buildClient()
    private val blobContainerClient by lazy {
        if (blobServiceClient.getBlobContainerClient(blobContainer).exists()) {
            blobServiceClient.getBlobContainerClient(blobContainer)
        } else {
            blobServiceClient.createBlobContainer(blobContainer)
        }
    }

    fun uploadFromFile(path: Path, blobName: String = path.fileName.toString(), metadata: Map<String, String>? = null, tags: Map<String, String>? = null): String {
        log.info { "Uploading $path to blob $blobName" }

        val blobClient = blobContainerClient.getBlobClient(blobName)
        if (blobClient.exists()) {
            log.warn { "Blob $blobName already exists and it will be overwritten" }
        }

        val blobOutputStream = blobClient.blockBlobClient.getBlobOutputStream(true)
        BufferedOutputStream(blobOutputStream, BUFFER_SIZE).use {
            Files.copy(path, it)
        }

        if (!metadata.isNullOrEmpty()) {
            log.info { "Adding metadata to blob $blobName (${metadata})" }
            blobClient.setMetadata(metadata)
            log.debug { "Added metadata to blob $blobName" }
        }

        if (!tags.isNullOrEmpty()) {
            log.info { "Adding tags to blob $blobName (${tags})" }
            blobClient.tags = tags
            log.debug { "Added tags to blob $blobName" }
        }

        log.info { "Done uploading $path" }

        return blobClient.blobName
    }
}