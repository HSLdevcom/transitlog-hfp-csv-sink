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

        app.context.healthServer?.addCheck {
            val timeSinceLastHandled = Duration.ofNanos(System.nanoTime() - messageHandler.getLastHandledMessageTime())
            //There should be new messages coming in every second
            val healthy = timeSinceLastHandled < Duration.ofMinutes(15)

            if (!healthy) {
                log.warn { "Service unhealthy, last message handled ${timeSinceLastHandled.toMinutes()} minutes ago" }
            }

            return@addCheck healthy
        }
        app.context.healthServer?.addCheck {
            val timeSinceLastAcknowledged = Duration.ofNanos(System.nanoTime() - messageHandler.getLastAcknowledgedMessageTime())
            //Messages should be acknowledged hourly when files are uploaded
            val healthy = timeSinceLastAcknowledged < Duration.ofHours(2)

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