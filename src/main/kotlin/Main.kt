import fi.hsl.common.config.ConfigParser
import fi.hsl.common.pulsar.PulsarApplication
import fi.hsl.transitlog.MessageHandler
import mu.KotlinLogging

private val log = KotlinLogging.logger {}

fun main(args: Array<String>) {
    log.info { "Starting application" }

    val config = ConfigParser.createConfig()

    try {
        PulsarApplication.newInstance(config).use { app ->
            MessageHandler(app.context).use { messageHandler ->
                /*if (app.context.healthServer != null) {
                    log.info { "Healthcheck enabled" }
                    app.context.healthServer!!.addCheck(messageHandler::isHealthy)
                }*/

                app.launchWithHandler(messageHandler)
            }
        }
    } catch (e: Exception) {
        log.error(e) { "Exception at main" }
    }
}