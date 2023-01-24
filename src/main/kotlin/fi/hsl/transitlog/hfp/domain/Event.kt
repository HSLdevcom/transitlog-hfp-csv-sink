package fi.hsl.transitlog.hfp.domain

import fi.hsl.common.hfp.proto.Hfp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.*

data class Event(
    override val uuid: UUID,
    override val tst: OffsetDateTime,
    override val uniqueVehicleId: String? = null,
    override val eventType: String? = null,
    override val journeyType: String? = null,
    override val receivedAt: Instant? = null,
    override val topicPrefix: String? = null,
    override val topicVersion: String? = null,
    override val isOngoing: Boolean? = null,
    override val mode: String? = null,
    override val ownerOperatorId: Int? = null,
    override val vehicleNumber: Int? = null,
    override val routeId: String? = null,
    override val directionId: Int? = null,
    override val headsign: String? = null,
    override val journeyStartTime: LocalTime? = null,
    override val nextStopId: String? = null,
    override val geohashLevel: Int? = null,
    override val topicLatitude: Double? = null,
    override val topicLongitude: Double? = null,
    override val latitude: Double? = null,
    override val longitude: Double? = null,
    override val desi: String? = null,
    override val dir: Int? = null,
    override val oper: Int? = null,
    override val veh: Int? = null,
    override val tsi: Long? = null,
    override val spd: Double? = null,
    override val hdg: Int? = null,
    override val acc: Double? = null,
    override val dl: Int? = null,
    override val odo: Double? = null,
    override val drst: Boolean? = null,
    override val oday: LocalDate? = null,
    override val jrn: Int? = null,
    override val line: Int? = null,
    override val start: LocalTime? = null,
    override val locationQualityMethod: String? = null,
    override val stop: Int? = null,
    override val route: String? = null,
    override val occu: Int? = null,
    override val seq: Int? = null,
    override val drType: Int? = null
) : IEvent() {
    companion object {
        fun parse(hfpTopic: Hfp.Topic, hfpPayload: Hfp.Payload): Event {
            return Event(
                UUID.randomUUID(),
                OffsetDateTime.parse(hfpPayload.tst),
                if (hfpTopic.hasUniqueVehicleId()) { hfpTopic.uniqueVehicleId } else { null },
                if (hfpTopic.hasEventType()) { hfpTopic.eventType.toString() } else { null },
                if (hfpTopic.hasJourneyType()) { hfpTopic.journeyType.toString() } else { null },
                if (hfpTopic.hasReceivedAt()) { Instant.ofEpochMilli(hfpTopic.receivedAt) } else { null },
                if (hfpTopic.hasTopicPrefix()) { hfpTopic.topicPrefix } else { null },
                if (hfpTopic.hasTopicVersion()) { hfpTopic.topicVersion } else { null },
                if (hfpTopic.hasTemporalType()) { hfpTopic.temporalType == Hfp.Topic.TemporalType.ongoing } else { null },
                if (hfpTopic.hasTransportMode()) { hfpTopic.transportMode.toString() } else { null },
                if (hfpTopic.hasOperatorId()) { hfpTopic.operatorId } else { null },
                if (hfpTopic.hasVehicleNumber()) { hfpTopic.vehicleNumber } else { null },
                if (hfpTopic.hasRouteId()) { hfpTopic.routeId } else { null },
                if (hfpTopic.hasDirectionId()) { hfpTopic.directionId } else { null },
                if (hfpTopic.hasHeadsign()) { hfpTopic.headsign } else { null },
                if (hfpTopic.hasStartTime()) { LocalTime.parse(hfpTopic.startTime) } else { null },
                if (hfpTopic.hasNextStop()) { hfpTopic.nextStop } else { null },
                if (hfpTopic.hasGeohashLevel()) { hfpTopic.geohashLevel } else { null },
                if (hfpTopic.hasLatitude()) { hfpTopic.latitude } else { null },
                if (hfpTopic.hasLongitude()) { hfpTopic.longitude } else { null },
                if (hfpPayload.hasLat()) { hfpPayload.lat } else { null },
                if (hfpPayload.hasLong()) { hfpPayload.long } else { null },
                if (hfpPayload.hasDesi()) { hfpPayload.desi } else { null },
                if (hfpPayload.hasDir()) { hfpPayload.dir.toIntOrNull() } else { null },
                if (hfpPayload.hasOper()) { hfpPayload.oper } else { null },
                if (hfpPayload.hasVeh()) { hfpPayload.veh } else { null },
                if (hfpPayload.hasTsi()) { hfpPayload.tsi } else { null },
                if (hfpPayload.hasSpd()) { hfpPayload.spd } else { null },
                if (hfpPayload.hasHdg()) { hfpPayload.hdg } else { null },
                if (hfpPayload.hasAcc()) { hfpPayload.acc } else { null },
                if (hfpPayload.hasDl()) { hfpPayload.dl } else { null },
                if (hfpPayload.hasOdo()) { hfpPayload.odo } else { null },
                if (hfpPayload.hasDrst()) { hfpPayload.drst == 1 } else { null },
                if (hfpPayload.hasOday()) { LocalDate.parse(hfpPayload.oday) } else { null },
                if (hfpPayload.hasJrn()) { hfpPayload.jrn } else { null },
                if (hfpPayload.hasLine()) { hfpPayload.line } else { null },
                if (hfpPayload.hasStart()) { LocalTime.parse(hfpPayload.start) } else { null },
                if (hfpPayload.hasLoc()) { hfpPayload.loc.toString() } else { null },
                if (hfpPayload.hasStop()) { hfpPayload.stop } else { null },
                if (hfpPayload.hasRoute()) { hfpPayload.route } else { null },
                if (hfpPayload.hasOccu()) { hfpPayload.occu } else { null },
                if (hfpPayload.hasSeq()) { hfpPayload.seq } else { null },
                if (hfpPayload.hasDrType()) { hfpPayload.drType } else { null }
            )
        }
    }
}
