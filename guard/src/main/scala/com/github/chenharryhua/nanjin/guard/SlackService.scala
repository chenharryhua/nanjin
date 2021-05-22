package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto._
import io.circe.syntax._

import java.time.Instant

/** Notes: slack messages
  * [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, fields: List[SlackField], ts: Long = Instant.now().getEpochSecond)
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final class SlackService[F[_]] private (service: SimpleNotificationService[F]) extends AlertService[F] {

  override def alert(status: Status)(implicit F: Async[F]): F[Unit] = status match {
    case ServiceStarted(applicationName, serviceName) =>
      val msg = SlackNotification(
        applicationName,
        ":rocket:",
        List(
          Attachment(
            "good",
            List(
              SlackField("Service Name", serviceName, short = true),
              SlackField("Status", "(Re)Started", short = true))
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void

    case ServiceRestarting(applicationName, serviceName, willDelayAndRetry, error) =>
      val msg = SlackNotification(
        applicationName,
        s""":system_restore: The service experienced a *panic*
           |```${utils.mkExceptionString(error, 8)}```
           |and started to *recover* itself
           |the next attempt will happen in *${utils
          .mkDurationString(willDelayAndRetry.nextDelay)}* meanwhile the service is disfunctional.
           |*full exception can be found in log file*""".stripMargin,
        List(
          Attachment(
            "danger",
            List(
              SlackField("Service Name", serviceName, short = true),
              SlackField("Status", "Restarting", short = true),
              SlackField("Retries so far", willDelayAndRetry.retriesSoFar.toString, short = true),
              SlackField(
                "Cumulative Delay",
                s"${utils.mkDurationString(willDelayAndRetry.cumulativeDelay)}",
                short = true)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void

    case ServiceAbnormalStop(applicationName, serviceName, error) =>
      val msg = SlackNotification(
        applicationName,
        s""":open_mouth: The service received a *fatal error*
           |${utils.mkExceptionString(error)}""".stripMargin,
        List(
          Attachment(
            "danger",
            List(
              SlackField("Service Name", serviceName, short = true),
              SlackField("Status", "Stopped abnormally", short = true))))
      )
      service.publish(msg.asJson.noSpaces).attempt.void

    case ServiceHealthCheck(applicationName, serviceName, interval) =>
      val msg = SlackNotification(
        applicationName,
        ":gottarun:",
        List(
          Attachment(
            "good",
            List(
              SlackField("Service Name", serviceName, short = true),
              SlackField("HealthCheck Status", "Good", short = true),
              SlackField("Next check will happen in", utils.mkDurationString(interval), short = true)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).attempt.void
    case ActionRetrying(_, _, _, _, _) => F.unit
    case ActionFailed(applicationName, sn, RetriedAction(id, st, tz), alertMask, givingUp, notes, _) =>
      val msg = SlackNotification(
        applicationName,
        notes,
        List(
          Attachment(
            "danger",
            List(
              SlackField("Service Name", sn, short = true),
              SlackField("Number of retries", givingUp.totalRetries.toString, short = true),
              SlackField("took", s"${utils.mkDurationString(st, Instant.now())}", short = true),
              SlackField("Time Zone", tz.toString, short = true),
              SlackField("Action ID", id.toString, short = false)
            )
          ))
      )
      service.publish(msg.asJson.noSpaces).whenA(alertMask.alertFail).attempt.void

    case ActionSucced(applicationName, sn, RetriedAction(id, st, _), alertMask, notes) =>
      val msg = SlackNotification(
        applicationName,
        notes,
        List(
          Attachment(
            "good",
            List(
              SlackField("Service Name", sn, short = true),
              SlackField("took", s"${utils.mkDurationString(st, Instant.now())}", short = true),
              SlackField("Action ID", id.toString, short = false)
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
