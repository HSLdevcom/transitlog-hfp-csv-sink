package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.utils.TestSink
import org.junit.jupiter.api.io.TempDir
import org.mockito.kotlin.mock
import java.io.File
import java.nio.file.Files
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.random.Random
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime

@ExperimentalTime
class DWServiceTest {
    private lateinit var dwService: DWService

    @TempDir
    @JvmField
    var tempFolder: File? = null

    private val testDataStartTime = ZonedDateTime.of(LocalDate.of(2021, 1, 1), LocalTime.of(8, 0), ZoneId.of("Europe/Helsinki"))

    private fun generateTestData(): List<Hfp.Data>  = (1..1000).map { i ->
        Hfp.Data.newBuilder()
            .setTopic(
                Hfp.Topic.newBuilder()
                .setDirectionId(1)
                .setEventType(Hfp.Topic.EventType.VP)
                .setGeohashLevel(5)
                .setHeadsign("Test")
                .setJourneyType(Hfp.Topic.JourneyType.journey)
                .setLatitude(60.12345)
                .setLongitude(24.12345)
                .setNextStop("1")
                .setOperatorId(1)
                .setVehicleNumber(1)
                .setReceivedAt(testDataStartTime.plusSeconds(i.toLong()).toInstant().toEpochMilli())
                .setRouteId("1")
                .setStartTime("08:00")
                .setTemporalType(Hfp.Topic.TemporalType.ongoing)
                .setTransportMode(Hfp.Topic.TransportMode.bus)
                .setTopicPrefix("/hfp/")
                .setTopicVersion("v2")
                .setUniqueVehicleId("1/1")
                .setSchemaVersion(1))
            .setPayload(
                Hfp.Payload.newBuilder()
                .setAcc(Random.Default.nextDouble())
                .setDesi("1")
                .setDir("1")
                .setDl(Random.Default.nextInt())
                .setDrType(1)
                .setDrst(0)
                .setHdg(Random.Default.nextInt(360))
                .setJrn(1)
                .setLine(1)
                .setLat(Random.Default.nextDouble(-90.0, 90.0))
                .setLong(Random.Default.nextDouble(-180.0, 180.0))
                .setLoc(Hfp.Payload.LocationQualityMethod.GPS)
                .setOccu(0)
                .setOday("2021-01-01")
                .setOdo(Random.Default.nextDouble())
                .setOper(1)
                .setRoute("1")
                .setSpd(Random.Default.nextDouble())
                .setStart("08:00")
                .setTsi(testDataStartTime.plusSeconds(i.toLong()).toEpochSecond())
                .setTst(testDataStartTime.plusSeconds(i.toLong()).toOffsetDateTime().toString())
                .setVeh(1)
                .setSchemaVersion(1))
            .setSchemaVersion(1)
            .build()
    }

    @BeforeTest
    fun setup() {
        dwService = DWService(tempFolder!!.toPath(), 19, TestSink(), TestSink(), { }, emptyList())
    }

    @Test
    fun `Test writing files`() {
        generateTestData().forEach {
            dwService.addEvent(it, mock { })
        }

        Thread.sleep(30000)

        assertTrue { Files.list(tempFolder!!.toPath()).count() > 0 }
    }

}