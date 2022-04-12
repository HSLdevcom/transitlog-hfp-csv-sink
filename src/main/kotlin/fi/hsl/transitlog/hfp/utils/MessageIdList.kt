package fi.hsl.transitlog.hfp.utils

import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException

/**
 * Helper class for saving a list of Pulsar message IDs
 */
class MessageIdList(estimatedSizeBytes: Int = 5 * 1024 * 1024) : Closeable {
    private val byteArrayOutputStream = ByteArrayOutputStream(estimatedSizeBytes)

    private val dataOutputStream = DataOutputStream(ZstdCompressorOutputStream(byteArrayOutputStream, 19))

    private var closed = false

    private var count = 0

    val size: Int
        get() = count

    val sizeBytes: Int
        get() = byteArrayOutputStream.size()

    fun addId(byteArray: ByteArray) {
        dataOutputStream.writeInt(byteArray.size)
        dataOutputStream.write(byteArray, 0, byteArray.size)

        count++
    }

    override fun close() {
        if (closed) {
            throw IllegalStateException("Already closed")
        }

        dataOutputStream.flush()
        dataOutputStream.close()

        closed = true
    }

    fun forEachId(handler: (ByteArray) -> Unit) {
        if (!closed) {
            throw IllegalStateException("Cannot get IDs if list is not closed for writing")
        }

        val dataInputStream = DataInputStream(ZstdCompressorInputStream(ByteArrayInputStream(byteArrayOutputStream.toByteArray())))

        while (true) {
            try {
                val nBytes = dataInputStream.readInt()
                handler(dataInputStream.readNBytes(nBytes))
            } catch (eof: EOFException) {
                //EOF is expected
                break
            }
        }
    }
}