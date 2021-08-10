package fi.hsl.transitlog.hfp.azure

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
            blobServiceClient.getBlobContainerClient(blobContainer)
        }
    }

    fun uploadFromFile(path: Path, blobName: String = path.fileName.toString()): String {
        log.info { "Uploading $path to blob $blobName" }

        val blobClient = blobContainerClient.getBlobClient(blobName)

        val blobOutputStream = blobClient.blockBlobClient.getBlobOutputStream(true)
        BufferedOutputStream(blobOutputStream, BUFFER_SIZE).use {
            Files.copy(path, it)
        }

        log.info { "Done uploading $path" }
        return blobClient.blobName
    }
}