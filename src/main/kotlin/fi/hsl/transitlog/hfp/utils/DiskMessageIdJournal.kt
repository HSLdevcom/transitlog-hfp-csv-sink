package fi.hsl.transitlog.hfp.utils

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

/** Append-only on-disk storage for serialized Pulsar message IDs. */
class DiskMessageIdJournal private constructor(
    private val path: Path,
    private var output: DataOutputStream?
) : AutoCloseable {
    companion object {
        private const val BUFFER_SIZE = 64 * 1024
        private const val MAX_ID_SIZE = 1024 * 1024

        fun create(path: Path): DiskMessageIdJournal {
            val output =
                DataOutputStream(
                    BufferedOutputStream(
                        Files.newOutputStream(
                            path,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE
                        ),
                        BUFFER_SIZE
                    )
                )
            return DiskMessageIdJournal(path, output)
        }

        fun openForReading(path: Path): DiskMessageIdJournal =
            DiskMessageIdJournal(path, null)
    }

    private var count = 0
    private var closed = output == null

    val size: Int
        get() = count

    fun appendId(bytes: ByteArray) {
        require(bytes.isNotEmpty() && bytes.size <= MAX_ID_SIZE) {
            "Serialized message ID size must be between 1 and $MAX_ID_SIZE bytes"
        }
        val stream = checkNotNull(output) { "Journal is closed" }
        stream.writeInt(bytes.size)
        stream.write(bytes)
        count++
    }

    override fun close() {
        if (!closed) {
            checkNotNull(output).close()
            output = null
            closed = true
        }
        FileChannel.open(path, StandardOpenOption.WRITE).use { it.force(true) }
    }

    fun forEachId(handler: (ByteArray) -> Unit): Int {
        check(closed) { "Cannot read a journal that is still open for writing" }

        var idsRead = 0
        DataInputStream(BufferedInputStream(Files.newInputStream(path), BUFFER_SIZE)).use { input ->
            while (true) {
                val firstByte = input.read()
                if (firstByte < 0) {
                    break
                }
                val length =
                    (firstByte shl 24) or
                        (input.readUnsignedByte() shl 16) or
                        (input.readUnsignedByte() shl 8) or
                        input.readUnsignedByte()

                require(length in 1..MAX_ID_SIZE) { "Invalid serialized message ID size: $length" }
                val bytes = ByteArray(length)
                input.readFully(bytes)
                handler(bytes)
                idsRead++
            }
        }
        return idsRead
    }
}
