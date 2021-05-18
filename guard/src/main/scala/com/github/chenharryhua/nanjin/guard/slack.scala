package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.utils
import io.circe.Codec
import io.circe.generic.auto._

import java.time.LocalDateTime

/** Notes: slack messages
  * [[https://api.slack.com/docs/messages/builder]]
  */
final case class SlackField(title: String, value: String, short: Boolean)
final case class Attachment(color: String, title: String, fields: List[SlackField])

final case class SlackNotification(username: String, text: String, attachments: List[Attachment])

object SlackNotification {
  implicit val codec: Codec[SlackNotification] = io.circe.generic.semiauto.deriveCodec[SlackNotification]
}

final private class Slack(applicationName: ApplicationName, serviceName: ServiceName) {
  val name: String = s"${applicationName.value}/${serviceName.value}"

  val start: SlackNotification =
    SlackNotification(
      applicationName.value,
      ":rocket:",
      List(
        Attachment(
          "good",
          "",
          List(
            SlackField("Service Name", serviceName.value, short = true),
            SlackField("Status", "(Re)Started", short = true))))
    )

  val shouldNotStop: SlackNotification =
    SlackNotification(
      applicationName.value,
      ":open_mouth:",
      List(
        Attachment(
          "danger",
          "",
          List(
            SlackField("Service Name", serviceName.value, short = true),
            SlackField("Status", "Stopped", short = true),
            SlackField("The service was unexpectedly stopped", "please contact deverloper", short = true)
          )
        ))
    )

  def healthCheck(interval: HealthCheckInterval): SlackNotification =
    SlackNotification(
      applicationName.value,
      ":gottarun:",
      List(
        Attachment(
          "good",
          "",
          List(
            SlackField("Service Name", serviceName.value, short = true),
            SlackField("Health Check Status", "Good", short = true),
            SlackField("Next check will happen in", s"${interval.value.toHours} hours", short = true)
          )
        ))
    )

  def foreverAlert(rfs: RetryForeverState): SlackNotification = {
    val nextAlert = (rfs.nextRetryIn * rfs.alertEveryNRetry.value.toLong).toMinutes
    SlackNotification(
      applicationName.value,
      s"""```${utils.mkString(rfs.err)}```""",
      List(
        Attachment(
          "danger",
          "",
          List(
            SlackField("Service Panic at", s"${LocalDateTime.now()}", short = true),
            SlackField("Service Name", serviceName.value, short = true),
            SlackField("Number of retries so far", rfs.numOfRetries.toString, short = true),
            SlackField(
              "The service will keep retrying until recovered",
              s"""|Next attempt will happen in ${rfs.nextRetryIn.toSeconds} seconds. 
                  |Next alert will be emitted in $nextAlert minutes in case that
                  |the service is not recovered from failure automatically.""".stripMargin,
              short = false
            )
          )
        ))
    )
  }

  def limitAlert(lrs: LimitedRetryState): SlackNotification =
    SlackNotification(
      applicationName.value,
      s"""```${utils.mkString(lrs.err)}```""",
      List(
        Attachment(
          "danger",
          "",
          List(
            SlackField("Service Panic at", s"${LocalDateTime.now()}", short = true),
            SlackField("Service Name", serviceName.value, short = true),
            SlackField("with Input", s"```${lrs.input}```", short = true),
            SlackField("Number of retries", lrs.totalRetries.toString, short = true),
            SlackField("Retries took", s"${lrs.totalDelay.toSeconds} seconds", short = true),
            SlackField("The action was failed", "", short = false)
          )
        ))
    )
}
