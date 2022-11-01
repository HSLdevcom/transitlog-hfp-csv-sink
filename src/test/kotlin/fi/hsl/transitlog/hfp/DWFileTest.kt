package fi.hsl.transitlog.hfp

import fi.hsl.common.hfp.proto.Hfp
import fi.hsl.transitlog.hfp.domain.Event
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream
import org.junit.jupiter.api.Test
import java.lang.RuntimeException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.*
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.asserter

class DWFileTest {
    private val testDataStartTime = ZonedDateTime.of(LocalDate.of(2021, 1, 1), LocalTime.of(8, 0), ZoneId.of("Europe/Helsinki"))

    private fun generateTestData(): List<Hfp.Data>  = (1..10).map { i ->
        Hfp.Data.newBuilder()
            .setTopic(Hfp.Topic.newBuilder()
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
            .setPayload(Hfp.Payload.newBuilder()
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

    @Test
    fun `Test writing events`() {
        val testData = generateTestData()

        val dwFile = DWFile.createDWFile(testData[0], compressionLevel = 19)
        try {
            testData.forEach { dwFile.writeEvent(Event.parse(it.topic, it.payload)) }

            dwFile.close()

            assertEquals("2021-01-01T08-1_VP.csv.zst", dwFile.blobName)
            assertFalse(dwFile.private)
            assertTrue(Files.size(dwFile.path) > 0, "File size greater than 0")

            ZstdCompressorInputStream(Files.newInputStream(dwFile.path)).use {
                val lines = it.bufferedReader(StandardCharsets.UTF_8).readLines()
                assertEquals(11, lines.size)
            }
        } catch (e: Exception) {
            throw e
        } finally {
            Files.delete(dwFile.path)
        }
    }
}