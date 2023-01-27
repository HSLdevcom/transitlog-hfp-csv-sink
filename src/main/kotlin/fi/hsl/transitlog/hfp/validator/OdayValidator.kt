package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.IEvent
import mu.KotlinLogging
import java.time.ZoneId

private val log = KotlinLogging.logger {}

class OdayValidator(private val timezone: ZoneId, private val maxPast: Int, private val maxFuture: Int) : EventValidator {
    override fun isValidEvent(event: IEvent): Boolean {
        //Only "journey" type events should have oday value
        if (event.oday == null && event.journeyType != "journey") {
            return true
        }

        val oday = event.oday!!

        val receivedAtDay = event.receivedAt!!.atZone(timezone).toLocalDate()!!
        val lowerBound = receivedAtDay.minusDays(maxPast.toLong())
        val upperBound = receivedAtDay.plusDays(maxFuture.toLong())

        val valid = oday >= lowerBound && oday <= upperBound
        if (!valid) {
            log.debug { "Oday ($oday) was outside of accepted range [$lowerBound - $upperBound] for vehicle ${event.uniqueVehicleId}" }
        }

        return valid
    }
}