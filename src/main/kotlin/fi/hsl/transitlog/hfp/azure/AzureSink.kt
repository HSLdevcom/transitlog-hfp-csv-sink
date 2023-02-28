package fi.hsl.transitlog.hfp.azure

import fi.hsl.transitlog.hfp.CSVSink
import java.nio.file.Path

class AzureSink(private val blobUploader: BlobUploader) : CSVSink {
    override fun upload(path: Path, name: String, metadata: Map<String, String>) {
        blobUploader.uploadFromFile(path, name, metadata)
    }
}