package fi.hsl.transitlog.sink

import java.nio.file.Path

interface Sink {
    fun upload(path: Path, name: String, metadata: Map<String, String>? = null, tags: Map<String, String>? = null)
}