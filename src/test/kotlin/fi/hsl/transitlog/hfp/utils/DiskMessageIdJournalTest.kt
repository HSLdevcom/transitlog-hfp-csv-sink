package fi.hsl.transitlog.hfp.utils

import java.io.EOFException
import java.nio.file.Path
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class DiskMessageIdJournalTest {
    @TempDir lateinit var tempDir: Path

    @Test
    fun `journal can be read after being closed`() {
        val path = tempDir.resolve("messages.msgids")
        val journal = DiskMessageIdJournal.create(path)
        val expected = listOf(byteArrayOf(1, 2, 3), byteArrayOf(4, 5))

        expected.forEach(journal::appendId)
        journal.close()

        val actual = mutableListOf<ByteArray>()
        val count = DiskMessageIdJournal.openForReading(path).forEachId(actual::add)

        assertEquals(expected.size, count)
        expected.indices.forEach { assertContentEquals(expected[it], actual[it]) }
    }

    @Test
    fun `truncated ID record is rejected`() {
        val path = tempDir.resolve("truncated.msgids")
        val journal = DiskMessageIdJournal.create(path)
        journal.appendId(byteArrayOf(1, 2, 3))
        journal.close()

        java.nio.file.Files.write(path, byteArrayOf(0), java.nio.file.StandardOpenOption.APPEND)

        assertFailsWith<EOFException> {
            DiskMessageIdJournal.openForReading(path).forEachId {}
        }
    }
}
