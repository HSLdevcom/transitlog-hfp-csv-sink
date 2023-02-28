package fi.hsl.transitlog.hfp.utils

import fi.hsl.transitlog.hfp.CSVSink
import mu.KotlinLogging
import java.nio.file.Files
import java.nio.file.Path

/**
 * Sink that only logs information about the files when "uploading", nothing else
 */
class TestSink : CSVSink {
    private val log = KotlinLogging.logger {}

    override fun upload(path: Path, name: String, metadata: Map<String, String>) {
        val fileSizeMiB = Files.size(path) / (1024 * 1024)
        log.info { "$path ($fileSizeMiB MiB), blob name: $name, metadata: $metadata" }
    }
}