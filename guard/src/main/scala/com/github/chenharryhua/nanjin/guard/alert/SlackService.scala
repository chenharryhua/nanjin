package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.common.utils
import io.circe.generic.auto._
import io.circe.syntax._

import java.time.LocalTime
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final private class SlackService[F[_]](service: SimpleNotificationService[F])(implicit F: Sync[F])
    extends AlertService[F] {

  override def alert(event: NJEvent): F[Unit] = event match {
    case ServiceStarted(info) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          info.appName,
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
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          info.appName,
          s""":system_restore: The service experienced a panic and started to *recover* itself
             |$upcomingDelay 
             |full exception can be found in log file by *Error ID*""".stripMargin,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service", info.serviceName, short = true),
                SlackField("Status", "Restarting", short = true),
                SlackField("Cause", error.message, short = true),
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

    case ServiceStopped(info) =>
      val msg = F.realTimeInstant.map { ts =>
        val stopped =
          if (info.params.isNormalStop)
            SlackNotification(
              info.appName,
              s":octagonal_sign: The service was stopped.",
              List(
                Attachment(
                  "good",
                  ts.toEpochMilli,
                  List(
                    SlackField("Service", info.serviceName, short = true),
                    SlackField("Launch Time", info.launchTime.toString, short = true),
                    SlackField("Up Time", utils.mkDurationString(info.launchTime, ts), short = true),
                    SlackField("Status", "Stopped", short = true)
                  )
                ))
            )
          else
            SlackNotification(
              info.appName,
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
            )
        stopped.asJson.noSpaces
      }
      msg.flatMap(service.publish).void

    case ServiceHealthCheck(info, dailySummaries) =>
      val msg = F.realTimeInstant.map { ts =>
        val diff = ts.atZone(info.params.zoneId).toLocalTime.toSecondOfDay.toLong -
          LocalTime.of(info.params.dailySummaryReset, 0).toSecondOfDay
        val dur = if (diff >= 0) diff else diff + 24 * 3600
        SlackNotification(
          info.appName,
          s""":gottarun: In past ${utils.mkDurationString(FiniteDuration(dur, TimeUnit.SECONDS))}
             |>Service Panic : ${dailySummaries.servicePanic}
             |>Action Failed : ${dailySummaries.actionFail}
             |>Action Retried: ${dailySummaries.actionRetries}
             |>Action Succed : ${dailySummaries.actionSucc}
             |""".stripMargin,
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
        ).asJson.noSpaces
      }
      msg.flatMap(service.publish).whenA(info.params.healthCheck.isEnabled)

    case ActionRetrying(_, _, _) => F.unit

    case ActionFailed(action, givingUp, endAt, notes, error) =>
      val msg = F.realTimeInstant.map(ts =>
        SlackNotification(
          action.appName,
          notes.value,
          List(
            Attachment(
              "danger",
              ts.toEpochMilli,
              List(
                SlackField("Service", action.serviceName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Took", utils.mkDurationString(action.launchTime, endAt), short = true),
                SlackField("Cause", error.message, short = true),
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
          action.appName,
          notes.value,
          List(
            Attachment(
              "good",
              ts.toEpochMilli,
              List(
                SlackField("Service", action.serviceName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Took", utils.mkDurationString(action.launchTime, endAt), short = true),
                SlackField("Retries", s"$numRetries/${action.params.maxRetries}", short = true),
                SlackField("Action ID", action.id.toString, short = false)
              )
            ))
        ).asJson.noSpaces)
      msg.flatMap(service.publish).whenA(action.params.alertMask.alertSucc)

    case ForYouInformation(message) => service.publish(message).void
  }
}

object SlackService {

  def apply[F[_]: Sync](topic: SnsArn, region: Regions): AlertService[F] =
    new SlackService[F](SimpleNotificationService(topic, region))

  def apply[F[_]: Sync](topic: SnsArn): AlertService[F] =
    new SlackService[F](SimpleNotificationService(topic))

  def apply[F[_]: Sync](service: SimpleNotificationService[F]): AlertService[F] =
    new SlackService[F](service)

}
