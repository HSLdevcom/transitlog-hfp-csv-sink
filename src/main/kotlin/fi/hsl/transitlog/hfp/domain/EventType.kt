package fi.hsl.transitlog.hfp.domain

import fi.hsl.common.hfp.proto.Hfp
import mu.KotlinLogging
import kotlin.reflect.KClass
import kotlin.reflect.full.declaredMemberProperties

enum class EventType(private val dataClass: KClass<*>) {
    VehiclePosition(fi.hsl.transitlog.hfp.domain.Event::class), StopEvent(fi.hsl.transitlog.hfp.domain.Event::class), LightPriorityEvent(fi.hsl.transitlog.hfp.domain.LightPriorityEvent::class), OtherEvent(fi.hsl.transitlog.hfp.domain.Event::class), UnsignedEvent(fi.hsl.transitlog.hfp.domain.Event::class);

    val csvHeader by lazy { dataClass.declaredMemberProperties.sortedBy { it.name }.map { it.name } }

    companion object {
        private val log = KotlinLogging.logger {}

        fun getEventType(hfpTopic: Hfp.Topic): EventType? {
            return when(hfpTopic.eventType) {
                Hfp.Topic.EventType.VP -> {
                    if (hfpTopic.journeyType == Hfp.Topic.JourneyType.deadrun) {
                        UnsignedEvent
                    } else {
                        VehiclePosition
                    }
                }
                Hfp.Topic.EventType.DUE,
                Hfp.Topic.EventType.ARR,
                Hfp.Topic.EventType.ARS,
                Hfp.Topic.EventType.PDE,
                Hfp.Topic.EventType.DEP,
                Hfp.Topic.EventType.PAS,
                Hfp.Topic.EventType.WAIT -> {
                    StopEvent
                }
                Hfp.Topic.EventType.TLR,
                Hfp.Topic.EventType.TLA -> {
                    LightPriorityEvent
                }
                Hfp.Topic.EventType.DOO,
                Hfp.Topic.EventType.DOC,
                Hfp.Topic.EventType.DA,
                Hfp.Topic.EventType.DOUT,
                Hfp.Topic.EventType.BA,
                Hfp.Topic.EventType.BOUT,
                Hfp.Topic.EventType.VJA,
                Hfp.Topic.EventType.VJOUT -> {
                    OtherEvent
                }
                else -> {
                    log.warn { "Received HFP message with unknown event type ${hfpTopic.eventType}" }
                    null
                }
            }
        }
    }
}