package fi.hsl.transitlog.hfp.utils

import java.math.BigInteger
import java.security.MessageDigest
import kotlin.random.Random
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue


class DeduplicatorTest {
    private lateinit var deduplicator: Deduplicator<String, String>

    private fun md5(input: String): String {
        val md = MessageDigest.getInstance("MD5")
        return BigInteger(1, md.digest(input.toByteArray(Charsets.UTF_8))).toString(16).padStart(32, '0')
    }

    @BeforeTest
    fun setup() {
        deduplicator = Deduplicator(::md5)
    }

    @Test
    fun `Test that duplicated values are consumed only once`() {
        var consumed = 0

        val string = "fnosidafnaspmfpoian"

        deduplicator.consumeOnlyOnce(string) { consumed++ }
        deduplicator.consumeOnlyOnce(string) { consumed++ }
        deduplicator.consumeOnlyOnce(string) { consumed++ }

        assertEquals(1, consumed)
    }

    @Test
    fun `Test that different values can be consumed`() {
        var consumedA = false
        var consumedB = false
        deduplicator.consumeOnlyOnce("a") { consumedA = true }
        deduplicator.consumeOnlyOnce("b") { consumedB = true }

        assertTrue(consumedA)
        assertTrue(consumedB)
    }
}