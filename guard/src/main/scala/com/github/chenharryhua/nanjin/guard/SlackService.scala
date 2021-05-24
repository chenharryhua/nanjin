package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto._
import io.circe.syntax._

/** Notes: slack messages
  * [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final class SlackService[F[_]] private (service: SimpleNotificationService[F]) extends AlertService[F] {

  override def alert(status: Status)(implicit F: Sync[F]): F[Unit] = status match {
    case ServiceStarted(applicationName, serviceName, _) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          ":rocket:",
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", serviceName, short = true),
                SlackField("Status", "(Re)Started", short = true))
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServicePanic(applicationName, serviceName, retryPolicy, startTime, willDelayAndRetry, error) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
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
              ts.toEpochMilli,
              List(
                SlackField("Service Name", serviceName, short = true),
                SlackField("Status", "Restarting", short = true),
                SlackField("Launch Time", startTime.toString, short = true),
                SlackField("Up Time", utils.mkDurationString(startTime, ts), short = true),
                SlackField("Retry Policy", retryPolicy, short = true),
                SlackField("Retries so far", willDelayAndRetry.retriesSoFar.toString, short = true),
                SlackField("Cumulative Delay", utils.mkDurationString(willDelayAndRetry.cumulativeDelay), short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServiceAbnormalStop(applicationName, serviceName, startTime, error) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          s""":open_mouth: The service received a *fatal error*
             |${utils.mkExceptionString(error)}""".stripMargin,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", serviceName, short = true),
                SlackField("Launch Time", startTime.toString, short = true),
                SlackField("Up Time", utils.mkDurationString(startTime, ts), short = true),
                SlackField("Status", "Stopped abnormally", short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServiceHealthCheck(applicationName, serviceName, interval, startTime) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          ":gottarun:",
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", serviceName, short = true),
                SlackField("HealthCheck Status", "Good", short = true),
                SlackField("Up Time", utils.mkDurationString(startTime, ts), short = true),
                SlackField("Next check will happen in", utils.mkDurationString(interval), short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ActionRetrying(_, _, _, _) => F.unit

    case ActionFailed(applicationName, retriedAction, givingUp, notes, _) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          notes,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Action Name", retriedAction.actionName, short = true),
                SlackField("Took", utils.mkDurationString(retriedAction.startTime, ts), short = true),
                SlackField("Retries", givingUp.totalRetries.toString, short = true),
                SlackField("Retry Policy", retriedAction.retryPolicy, short = true),
                SlackField("Action ID", retriedAction.actionID.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(retriedAction.alertMask.alertFail)

    case ActionSucced(applicationName, retriedAction, notes, retries) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          notes,
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Action Name", retriedAction.actionName, short = true),
                SlackField("Took", utils.mkDurationString(retriedAction.startTime, ts), short = true),
                SlackField("Retries", retries.toString, short = true),
                SlackField("Action ID", retriedAction.actionID.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(retriedAction.alertMask.alertSucc)
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
