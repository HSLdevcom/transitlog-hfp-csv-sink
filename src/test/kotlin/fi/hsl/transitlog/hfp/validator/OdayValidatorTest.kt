package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.Event
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OdayValidatorTest {
    private val TIMEZONE = ZoneId.of("Europe/Helsinki")

    private lateinit var odayValidator: OdayValidator

    @BeforeTest
    fun setup() {
        odayValidator = OdayValidator(TIMEZONE, 1, 1)
    }

    @Test
    fun `Test validating valid date`() {
        val date = LocalDate.of(2023, 1, 1)

        val time = ZonedDateTime.of(date.plusDays(1), LocalTime.of(7, 30), TIMEZONE).toOffsetDateTime()

        val event = Event(
            UUID.randomUUID(),
            time,
            receivedAt = time.toInstant(),
            oday = date
        )

        assertTrue { odayValidator.isValidEvent(event) }
    }

    @Test
    fun `Test validating invalid date`() {
        val date = LocalDate.of(2023, 1, 1)

        val time = ZonedDateTime.of(date.plusDays(7), LocalTime.of(7, 30), TIMEZONE).toOffsetDateTime()

        val event = Event(
            UUID.randomUUID(),
            time,
            receivedAt = time.toInstant(),
            oday = date
        )

        assertFalse { odayValidator.isValidEvent(event) }
    }
}