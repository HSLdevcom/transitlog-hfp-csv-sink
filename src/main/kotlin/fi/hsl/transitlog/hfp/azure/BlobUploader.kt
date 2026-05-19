package fi.hsl.transitlog.hfp.azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.BlobServiceClient
import com.azure.storage.blob.BlobServiceClientBuilder
import java.io.BufferedOutputStream
import java.nio.file.Files
import java.nio.file.Path
import mu.KotlinLogging

private const val BUFFER_SIZE = 65536

class BlobUploader
private constructor(
    private val blobServiceClient: BlobServiceClient,
    private val blobContainer: String
) {
    private val log = KotlinLogging.logger {}

    companion object {
        fun withDefaultAzureCredential(
            blobAccountName: String,
            blobContainer: String
        ): BlobUploader {
            val client =
                BlobServiceClientBuilder()
                    .endpoint("https://$blobAccountName.blob.core.windows.net")
                    .credential(DefaultAzureCredentialBuilder().build())
                    .buildClient()

            return BlobUploader(client, blobContainer)
        }

        fun withConnectionString(connectionString: String, blobContainer: String): BlobUploader {
            val client = BlobServiceClientBuilder().connectionString(connectionString).buildClient()

            return BlobUploader(client, blobContainer)
        }
    }

    private val blobContainerClient by lazy {
        val containerClient = blobServiceClient.getBlobContainerClient(blobContainer)

        if (containerClient.exists()) {
            containerClient
        } else {
            blobServiceClient.createBlobContainer(blobContainer)
        }
    }

    fun uploadFromFile(
        path: Path,
        blobName: String = path.fileName.toString(),
        metadata: Map<String, String> = emptyMap()
    ): String {
        log.info { "Uploading $path to blob $blobName" }

        val blobClient = blobContainerClient.getBlobClient(blobName)
        if (blobClient.exists()) {
            log.warn { "Blob $blobName already exists and it will be overwritten" }
        }

        val blobOutputStream = blobClient.blockBlobClient.getBlobOutputStream(true)
        BufferedOutputStream(blobOutputStream, BUFFER_SIZE).use { Files.copy(path, it) }

        if (metadata.isNotEmpty()) {
            log.info { "Adding tags to blob $blobName (${metadata})" }
            blobClient.tags = metadata
            log.debug { "Added tags to blob $blobName" }
        }

        log.info { "Done uploading $path" }

        return blobClient.blobName
    }
}
