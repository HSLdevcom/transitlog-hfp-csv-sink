package fi.hsl.transitlog.hfp.domain

import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.OffsetDateTime
import java.util.*

sealed class IEvent {
    abstract val uuid: UUID
    abstract val tst: OffsetDateTime
    abstract val uniqueVehicleId: String?
    abstract val eventType: String?
    abstract val journeyType: String?
    abstract val receivedAt: Instant?
    abstract val topicPrefix: String?
    abstract val topicVersion: String?
    abstract val isOngoing: Boolean?
    abstract val mode: String?
    abstract val ownerOperatorId: Int?
    abstract val vehicleNumber: Int?
    abstract val routeId: String?
    abstract val directionId: Int?
    abstract val headsign: String?
    abstract val journeyStartTime: LocalTime?
    abstract val nextStopId: String?
    abstract val geohashLevel: Int?
    abstract val topicLatitude: Double?
    abstract val topicLongitude: Double?
    abstract val latitude: Double?
    abstract val longitude: Double?
    abstract val desi: String?
    abstract val dir: Int?
    abstract val oper: Int?
    abstract val veh: Int?
    abstract val tsi: Long?
    abstract val spd: Double?
    abstract val hdg: Int?
    abstract val acc: Double?
    abstract val dl: Int?
    abstract val odo: Double?
    abstract val drst: Boolean?
    abstract val oday: LocalDate?
    abstract val jrn: Int?
    abstract val line: Int?
    abstract val start: LocalTime?
    abstract val locationQualityMethod: String?
    abstract val stop: Int?
    abstract val route: String?
    abstract val occu: Int?
    abstract val seq: Int?
    abstract val drType: Int?
}