package fi.hsl.transitlog.sink.azure

import fi.hsl.transitlog.sink.Sink
import java.nio.file.Path

class AzureSink(private val blobUploader: BlobUploader) : Sink {
    override fun upload(path: Path, name: String, metadata: Map<String, String>?, tags: Map<String, String>?) {
        blobUploader.uploadFromFile(path, name, metadata, tags)
    }
}