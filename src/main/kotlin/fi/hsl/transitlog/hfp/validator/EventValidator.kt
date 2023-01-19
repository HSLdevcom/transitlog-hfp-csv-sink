package fi.hsl.transitlog.hfp.validator

import fi.hsl.transitlog.hfp.domain.IEvent

interface EventValidator {
    fun isValidEvent(event: IEvent): Boolean
}