package fi.hsl.transitlog.hfp

import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import mu.KotlinLogging
import java.time.Duration

private val log = KotlinLogging.logger {}

fun main(vararg args: String) {
    log.info { "Starting application" }

    val config = ConfigParser.createConfig()

    try {
        val app = PulsarApplication.newInstance(config)

        val messageHandler = MessageHandler(app.context)

        //Service is considered unhealthy if no message is received within this duration
        val unhealthyIfNoMessage = app.context.config!!.getDuration("application.unhealthyIfNoMessage")
        //Service is considered unhealthy if no message is acknowledged within this duration
        //Use long enough duration for this value, files contain data for one hour and are uploaded after they have not been modified for one hour
        val unhealthyIfNoAck = app.context.config!!.getDuration("application.unhealthyIfNoAck")

        app.context.healthServer?.addCheck {
            val timeSinceLastHandled = Duration.ofNanos(System.nanoTime() - messageHandler.getLastHandledMessageTime())
            //There should be new messages coming in every second
            val healthy = timeSinceLastHandled < unhealthyIfNoMessage

            if (!healthy) {
                log.warn { "Service unhealthy, last message handled ${timeSinceLastHandled.toMinutes()} minutes ago" }
            }

            return@addCheck healthy
        }
        app.context.healthServer?.addCheck {
            val timeSinceLastAcknowledged = Duration.ofNanos(System.nanoTime() - messageHandler.getLastAcknowledgedMessageTime())
            //Messages should be acknowledged hourly when files are uploaded
            val healthy = timeSinceLastAcknowledged < unhealthyIfNoAck

            if (!healthy) {
                log.warn { "Service unhealthy, last message acknowledged ${timeSinceLastAcknowledged.toMinutes()} minutes ago" }
            }

            return@addCheck healthy
        }

        app.launchWithHandler(messageHandler)
        log.info { "Started handling messages" }
    } catch (e: Exception) {
        log.error(e) { "Exception at main" }
    }
}