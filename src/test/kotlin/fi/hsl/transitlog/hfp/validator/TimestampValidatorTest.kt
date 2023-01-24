package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.Event
import java.time.Duration
import java.time.OffsetDateTime
import java.util.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TimestampValidatorTest {
    private lateinit var timestampValidator: TimestampValidator

    @BeforeTest
    fun setup() {
        timestampValidator = TimestampValidator(Duration.ofHours(5), Duration.ofHours(5))
    }

    @Test
    fun `Test validating event with valid timestamp`() {
        val time = OffsetDateTime.now()

        val event = Event(
            UUID.randomUUID(),
            time,
            receivedAt = time.toInstant()
        )

        assertTrue { timestampValidator.isValidEvent(event) }
    }

    @Test
    fun `Test validating event with invalid timestamp`() {
        val time = OffsetDateTime.now()

        val event = Event(
            UUID.randomUUID(),
            time,
            receivedAt = time.plusHours(10).toInstant()
        )

        assertFalse { timestampValidator.isValidEvent(event) }
    }
}