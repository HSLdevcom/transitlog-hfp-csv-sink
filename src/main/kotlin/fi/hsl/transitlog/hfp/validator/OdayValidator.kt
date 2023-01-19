package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.IEvent
import java.time.ZoneId
import java.time.temporal.ChronoUnit

class OdayValidator(private val timezone: ZoneId, private val maxPast: Int, private val maxFuture: Int) : EventValidator {
    override fun isValidEvent(event: IEvent): Boolean {
        //Only "journey" type events should have oday value
        if (event.oday == null && event.journeyType != "journey") {
            return true
        }

        val oday = event.oday!!
        val receivedAtDay = event.receivedAt!!.atZone(timezone).toLocalDate()!!

        return oday.until(receivedAtDay, ChronoUnit.DAYS) <= maxPast && receivedAtDay.until(oday, ChronoUnit.DAYS) <= maxFuture
    }
}