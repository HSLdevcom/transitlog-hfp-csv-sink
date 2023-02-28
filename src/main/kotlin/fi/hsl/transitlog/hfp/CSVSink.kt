package fi.hsl.transitlog.hfp

import java.nio.file.Path

interface CSVSink {
    fun upload(path: Path, name: String, metadata: Map<String, String>)
}