package fi.hsl.transitlog.hfp.utils

import kotlin.test.assertTrue
import org.junit.jupiter.api.Test

class DaemonThreadFactoryTest {
    @Test
    fun `Test that thread is daemon`() {
        assertTrue(DaemonThreadFactory.newThread {}.isDaemon)
    }
}
