package fi.hsl.transitlog.hfp.utils

import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class DaemonThreadFactoryTest {
    @Test
    fun `Test that thread is daemon`() {
        assertTrue(DaemonThreadFactory.newThread { }.isDaemon)
    }
}