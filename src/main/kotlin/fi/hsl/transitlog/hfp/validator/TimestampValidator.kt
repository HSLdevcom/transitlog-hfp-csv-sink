package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.IEvent
import mu.KotlinLogging
import java.time.Duration
import java.time.format.DateTimeFormatter

private val log = KotlinLogging.logger {}

class TimestampValidator(private val maxPast: Duration, private val maxFuture: Duration) : EventValidator {
    override fun isValidEvent(event: IEvent): Boolean {
        if (event.receivedAt == null) {
            return false
        }
        val tstAsInstant = event.tst.toInstant()!!

        val lowerBound = event.receivedAt!!.minus(maxPast)
        val upperBound = event.receivedAt!!.plus(maxFuture)

        val valid = tstAsInstant >= lowerBound && tstAsInstant <= upperBound
        if (!valid) {
            log.debug { "Timestamp (tst: ${DateTimeFormatter.ISO_INSTANT.format(tstAsInstant)}) was outside of accepted range [${DateTimeFormatter.ISO_INSTANT.format(lowerBound)} - ${DateTimeFormatter.ISO_INSTANT.format(upperBound)}] for vehicle: ${event.uniqueVehicleId}" }
        }

        return valid
    }
}