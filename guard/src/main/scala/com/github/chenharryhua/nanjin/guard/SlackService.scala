package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.utils
import io.circe.generic.auto._
import io.circe.syntax._

import java.time.Instant

/** Notes: slack messages
  * [[https://api.slack.com/docs/messages/builder]]
  */
final case class SlackField(title: String, value: String, short: Boolean)

final case class Attachment(color: String, fields: List[SlackField], ts: Long = Instant.now().getEpochSecond)
final case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final class SlackService[F[_]] private (service: SimpleNotificationService[F]) extends AlertService[F] {

  override def alert(status: Status)(implicit F: Async[F]): F[Unit] = status match {
    case ServiceStarted(applicationName, serviceName) =>
      val msg = SlackNotification(
        applicationName.value,
        ":rocket:",
        List(
          Attachment(
            "good",
            List(
              SlackField("Service Name", serviceName.value, short = true),
              SlackField("Status", "(Re)Started", short = true))
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void

    case ServiceRestarting(applicationName, serviceName, willDelayAndRetry, error) =>
      val msg = SlackNotification(
        applicationName.value,
        s""":system_restore:```${utils.mkString(error)}```""",
        List(
          Attachment(
            "danger",
            List(
              SlackField("Service Name", serviceName.value, short = true),
              SlackField("Status", "Restarting", short = true),
              SlackField("Retries so far", willDelayAndRetry.retriesSoFar.toString, short = true),
              SlackField("Cumulative Delay", s"${willDelayAndRetry.cumulativeDelay.toMinutes} minutes", short = true),
              SlackField(
                "The service will keep retrying until recovered",
                s"""|Next attempt will happen in ${willDelayAndRetry.nextDelay.toSeconds} seconds 
                    |in case the service is not recovered automatically.""".stripMargin,
                short = false
              )
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void

    case ServiceAbnormalStop(applicationName, serviceName, error) =>
      val msg = SlackNotification(
        applicationName.value,
        s":open_mouth: ${error.getMessage}",
        List(
          Attachment(
            "danger",
            List(
              SlackField("Service Name", serviceName.value, short = true),
              SlackField("Status", "Stopped", short = true),
              SlackField("The service was unexpectedly stopped", "fatal error. restart does not help", short = true)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void

    case ServiceHealthCheck(applicationName, serviceName, interval) =>
      val msg = SlackNotification(
        applicationName.value,
        ":gottarun:",
        List(
          Attachment(
            "good",
            List(
              SlackField("Service Name", serviceName.value, short = true),
              SlackField("HealthCheck Status", "Good", short = true),
              SlackField("Next check will happen in", s"${interval.value.toHours} hours", short = true)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void
    case ActionRetrying(_, _, _, _, _, _, _) => F.unit
    case ActionFailed(applicationName, serviceName, actionID, actionInput, alertMask, error, givingUp) =>
      val msg = SlackNotification(
        applicationName.value,
        s""":oops:```${utils.mkString(error)}```""",
        List(
          Attachment(
            "danger",
            List(
              SlackField("Service Name", serviceName.value, short = true),
              SlackField("Status", "Failed", short = true),
              SlackField("Number of retries", givingUp.totalRetries.toString, short = true),
              SlackField("Retries took", s"${givingUp.totalDelay.toSeconds} seconds", short = true),
              SlackField("Action Input", s"```${actionInput.value}```", short = false),
              SlackField("Action ID", actionID.value.toString, short = false)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).whenA(alertMask.alertFail).attempt.void

    case ActionSucced(applicationName, serviceName, actionID, actionInput, alertMask) =>
      val msg = SlackNotification(
        applicationName.value,
        ":ok_hand:",
        List(
          Attachment(
            "good",
            List(
              SlackField("Service Name", serviceName.value, short = true),
              SlackField("Status", "Success", short = true),
              SlackField("Action Input", s"```${actionInput.value}```", short = false),
              SlackField("Action ID", actionID.value.toString, short = false)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).whenA(alertMask.alertSucc).attempt.void
  }
}

object SlackService {

  def apply[F[_]](topic: SnsArn, region: Regions): SlackService[F] =
    new SlackService[F](SimpleNotificationService(topic, region))

  def apply[F[_]](topic: SnsArn): SlackService[F] =
    apply[F](topic, Regions.AP_SOUTHEAST_2)

  def apply[F[_]](service: SimpleNotificationService[F]): SlackService[F] =
    new SlackService[F](service)

}
