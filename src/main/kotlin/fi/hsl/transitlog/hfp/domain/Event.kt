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
    override val uniqueVehicleId: String?,
    override val eventType: String?,
    override val journeyType: String?,
    override val receivedAt: Instant?,
    override val topicPrefix: String?,
    override val topicVersion: String?,
    override val isOngoing: Boolean?,
    override val mode: String?,
    override val ownerOperatorId: Int?,
    override val vehicleNumber: Int?,
    override val routeId: String?,
    override val directionId: Int?,
    override val headsign: String?,
    override val journeyStartTime: LocalTime?,
    override val nextStopId: String?,
    override val geohashLevel: Int?,
    override val topicLatitude: Double?,
    override val topicLongitude: Double?,
    override val latitude: Double?,
    override val longitude: Double?,
    override val desi: String?,
    override val dir: Int?,
    override val oper: Int?,
    override val veh: Int?,
    override val tsi: Long?,
    override val spd: Double?,
    override val hdg: Int?,
    override val acc: Double?,
    override val dl: Int?,
    override val odo: Double?,
    override val drst: Boolean?,
    override val oday: LocalDate?,
    override val jrn: Int?,
    override val line: Int?,
    override val start: LocalTime?,
    override val locationQualityMethod: String?,
    override val stop: Int?,
    override val route: String?,
    override val occu: Int?,
    override val seq: Int?,
    override val drType: Int?
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
