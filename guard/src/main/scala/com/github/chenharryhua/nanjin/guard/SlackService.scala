package com.github.chenharryhua.nanjin.guard

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.commons.lang3.exception.ExceptionUtils

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final class SlackService[F[_]] private (service: SimpleNotificationService[F])(implicit F: Sync[F])
    extends AlertService[F] {

  override def alert(event: NJEvent): F[Unit] = event match {
    case ServiceStarted(info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          info.applicationName,
          ":rocket:",
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service", info.serviceName, short = true),
                SlackField("Status", "(Re)Started", short = true))
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServicePanic(info, details, errorID, error) =>
      val upcomingDelay: String = details.upcomingDelay.map(utils.mkDurationString) match {
        case None     => "should never see this" // never happen
        case Some(ts) => s"next attempt will happen in *$ts* meanwhile the service is *dysfunctional*."
      }
      val cause = ExceptionUtils.getMessage(error)
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          info.applicationName,
          s""":system_restore: The service experienced a panic caused by *$cause* and started to *recover* itself
             |$upcomingDelay 
             |full exception can be found in log file by *Error ID*""".stripMargin,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service", info.serviceName, short = true),
                SlackField("Status", "Restarting", short = true),
                SlackField("Launch Time", info.launchTime.toString, short = true),
                SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                SlackField("Retry Policy", info.params.retryPolicy.policy[F].show, short = true),
                SlackField("Retries so far", details.retriesSoFar.toString, short = true),
                SlackField("Cumulative Delay", utils.mkDurationString(details.cumulativeDelay), short = true),
                SlackField("Error ID", errorID.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServiceStoppedAbnormally(info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          info.applicationName,
          s":octagonal_sign: The service was unexpectedly stopped. It is a *FATAL* error",
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service", info.serviceName, short = true),
                SlackField("Launch Time", info.launchTime.toString, short = true),
                SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                SlackField("Status", "Stopped abnormally", short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).void

    case ServiceHealthCheck(info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          info.applicationName,
          ":gottarun:",
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service", info.serviceName, short = true),
                SlackField("HealthCheck Status", "Good", short = true),
                SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                SlackField(
                  "Next check will happen in",
                  utils.mkDurationString(info.params.healthCheck.interval),
                  short = true)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(info.params.healthCheck.isEnabled)

    case ActionRetrying(_, _, _) => F.unit

    case ActionFailed(action, givingUp, endAt, notes, _) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          action.applicationName,
          Option(notes).getOrElse(""), // precaution
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service", action.parentName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Took", utils.mkDurationString(action.launchTime, endAt), short = true),
                SlackField("Retries", givingUp.totalRetries.toString, short = true),
                SlackField("Retry Policy", action.params.retryPolicy.policy[F].show, short = true),
                SlackField("Action ID", action.id.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(action.params.alertMask.alertFail)

    case ActionSucced(action, endAt, numRetries, notes) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          action.applicationName,
          Option(notes).getOrElse(""),
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service", action.parentName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Took", utils.mkDurationString(action.launchTime, endAt), short = true),
                SlackField("Retries", s"$numRetries/${action.params.maxRetries}", short = true),
                SlackField("Action ID", action.id.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(action.params.alertMask.alertSucc)

    case ForYouInformation(applicationName, message) =>
      service.publish(SlackNotification(applicationName, message, List.empty).asJson.noSpaces).void
  }
}

object SlackService {

  def apply[F[_]: Sync](topic: SnsArn, region: Regions): SlackService[F] =
    new SlackService[F](SimpleNotificationService(topic, region))

  def apply[F[_]: Sync](topic: SnsArn): SlackService[F] =
    apply[F](topic, Regions.AP_SOUTHEAST_2)

  def apply[F[_]: Sync](service: SimpleNotificationService[F]): SlackService[F] =
    new SlackService[F](service)

}
