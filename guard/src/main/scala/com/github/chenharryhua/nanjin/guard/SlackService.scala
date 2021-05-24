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
    case ServiceStarted(applicationName, info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          ":rocket:",
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", info.name, short = true),
                SlackField("Status", "(Re)Started", short = true))
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServicePanic(applicationName, info, details, error) =>
      val upcomingDelay: String = details.upcomingDelay.map(utils.mkDurationString) match {
        case None     => "The service was unexpectedly stopped. It is a *FATAL* error" // never happen
        case Some(ts) => s"next attempt will happen in *$ts* meanwhile the service is `dysfunctional`."
      }
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          s""":system_restore: The service experienced a *panic*
             |```${utils.mkExceptionString(error, 8)}```
             |and started to *recover* itself
             |$upcomingDelay 
             |full exception can be found in log file""".stripMargin,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", info.name, short = true),
                SlackField("Status", "Restarting", short = true),
                SlackField("Launch Time", info.launchTime.toString, short = true),
                SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                SlackField("Retry Policy", info.retryPolicy, short = true),
                SlackField("Retries so far", details.retriesSoFar.toString, short = true),
                SlackField("Cumulative Delay", utils.mkDurationString(details.cumulativeDelay), short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServiceAbnormalStop(applicationName, info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          s":open_mouth: The service was unexpectedly stopped. It is a *FATAL* error",
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", info.name, short = true),
                SlackField("Launch Time", info.launchTime.toString, short = true),
                SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                SlackField("Status", "Stopped abnormally", short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServiceHealthCheck(applicationName, info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          ":gottarun:",
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service Name", info.name, short = true),
                SlackField("HealthCheck Status", "Good", short = true),
                SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                SlackField("Next check will happen in", utils.mkDurationString(info.healthCheck), short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ActionRetrying(_, _, _, _) => F.unit

    case ActionFailed(applicationName, action, givingUp, notes, _) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          notes,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Action Name", action.name, short = true),
                SlackField("Took", utils.mkDurationString(action.launchTime, ts), short = true),
                SlackField("Retries", givingUp.totalRetries.toString, short = true),
                SlackField("Retry Policy", action.retryPolicy, short = true),
                SlackField("Action ID", action.id.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(action.alertMask.alertFail)

    case ActionSucced(applicationName, action, notes, retries) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          applicationName,
          notes,
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Action Name", action.name, short = true),
                SlackField("Took", utils.mkDurationString(action.launchTime, ts), short = true),
                SlackField("Retries", retries.toString, short = true),
                SlackField("Action ID", action.id.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(action.alertMask.alertSucc)

    case ForYouInformation(applicationName, message) =>
      service.publish(SlackNotification(applicationName, message, List.empty).asJson.noSpaces).void
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
