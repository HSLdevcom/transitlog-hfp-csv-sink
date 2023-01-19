package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.IEvent
import java.time.Duration

class TimestampValidator(private val maxPast: Duration, private val maxFuture: Duration) : EventValidator {
    override fun isValidEvent(event: IEvent): Boolean {
        if (event.receivedAt == null) {
            return false
        }
        val tstAsInstant = event.tst.toInstant()!!

        return tstAsInstant >= event.receivedAt!!.minus(maxPast) && tstAsInstant <= event.receivedAt!!.plus(maxFuture)
    }
}