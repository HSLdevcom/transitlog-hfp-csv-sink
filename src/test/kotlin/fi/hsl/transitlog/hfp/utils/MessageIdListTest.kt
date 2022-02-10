package fi.hsl.transitlog.hfp.utils

import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MessageIdListTest {
    @Test
    fun `Test message ID list`() {
        val messageIdList = MessageIdList()

        messageIdList.addId(byteArrayOf(1, 2, 3, 4))
        messageIdList.addId(byteArrayOf(1, 2, 3, 4))
        messageIdList.addId(byteArrayOf(5, 6, 7, 8))
        messageIdList.addId(byteArrayOf(5, 6, 7, 8))

        messageIdList.close()

        assertEquals(4, messageIdList.size)

        val ids = mutableListOf<ByteArray>()
        messageIdList.forEachId { bytes -> ids.add(bytes) }

        assertContentEquals(byteArrayOf(1, 2, 3, 4), ids[0])
        assertContentEquals(byteArrayOf(5, 6, 7, 8), ids[2])
    }

    @Test
    fun `Test data is compressed`() {
        val uncompressed = ByteArrayOutputStream()
        val messageIdList = MessageIdList()

        for (i in 0..10_000_000) {
            val bytes = byteArrayOf((i shr 0).toByte(), (i shr 8).toByte(), (i shr 16).toByte(), (i shr 24).toByte())
            messageIdList.addId(bytes)
            uncompressed.writeBytes(bytes)
        }

        messageIdList.close()
        uncompressed.close()

        assertTrue(messageIdList.sizeBytes < uncompressed.size())
    }
}