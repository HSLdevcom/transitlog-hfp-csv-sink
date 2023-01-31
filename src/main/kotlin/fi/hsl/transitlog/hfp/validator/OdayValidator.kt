package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.IEvent
import mu.KotlinLogging
import java.time.ZoneId

private val log = KotlinLogging.logger {}

class OdayValidator(private val timezone: ZoneId, private val maxPast: Int, private val maxFuture: Int) : EventValidator {
    override fun isValidEvent(event: IEvent): Boolean {
        if (event.oday == null) {
            //If oday is missing, the data is valid only if its journey type is not journey (i.e. the vehicle is on a deadrun)
            return event.journeyType != "journey"
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