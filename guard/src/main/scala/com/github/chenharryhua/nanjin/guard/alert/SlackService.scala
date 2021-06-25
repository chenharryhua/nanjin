package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.Sync
import cats.syntax.all._
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.commons.lang3.StringUtils
import squants.information.{Gigabytes, Megabytes}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final private class SlackService[F[_]](service: SimpleNotificationService[F], fmt: DurationFormatter)(implicit
  F: Sync[F])
    extends AlertService[F] {

  override def alert(event: NJEvent): F[Unit] = event match {

    case ServiceStarted(at, _, params) =>
      val msg = SlackNotification(
        params.taskParams.appName,
        s":rocket: ${params.notes}",
        List(
          Attachment(
            params.taskParams.color.succ,
            at.toInstant.toEpochMilli,
            List(
              SlackField("Service", params.serviceName, short = true),
              SlackField("Host", params.taskParams.hostName, short = true),
              SlackField("Status", "(Re)Started", short = true),
              SlackField("Time Zone", params.taskParams.zoneId.toString, short = true)
            )
          ))
      ).asJson.noSpaces
      service.publish(msg).void

    case ServicePanic(at, info, params, details, error) =>
      val upcoming: String = details.upcomingDelay.map(fmt.format) match {
        case None     => "should never see this" // never happen
        case Some(ts) => s"restart of which takes place in *$ts* meanwhile the service is dysfunctional."
      }
      val msg =
        SlackNotification(
          params.taskParams.appName,
          s""":system_restore: The service experienced a panic, $upcoming
             |Search *${error.id}* in log file to find full exception.""".stripMargin,
          List(
            Attachment(
              params.taskParams.color.fail,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Status", "Restarting", short = true),
                SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
                SlackField("Restarted so far", details.retriesSoFar.toString, short = true),
                SlackField("Retry Policy", params.retryPolicy.policy[F].show, short = true),
                SlackField("Cumulative Delay", fmt.format(details.cumulativeDelay), short = true),
                SlackField("Cause", StringUtils.abbreviate(error.message, params.maxCauseSize), short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).void

    case ServiceStopped(at, info, params) =>
      val msg =
        if (params.isNormalStop)
          SlackNotification(
            params.taskParams.appName,
            s":octagonal_sign: The service was stopped.",
            List(
              Attachment(
                params.taskParams.color.succ,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
                  SlackField("Status", "Stopped", short = true)
                )
              ))
          )
        else
          SlackNotification(
            params.taskParams.appName,
            s":octagonal_sign: The service was unexpectedly stopped. It is a *FATAL* error",
            List(
              Attachment(
                params.taskParams.color.fail,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
                  SlackField("Status", "Stopped abnormally", short = true)
                )
              ))
          )

      service.publish(msg.asJson.noSpaces).void

    case ServiceHealthCheck(at, info, params, dailySummaries, totalMemory, freeMemory) =>
      val msg = SlackNotification(
        params.taskParams.appName,
        s":gottarun: ${params.notes}",
        List(
          Attachment(
            params.taskParams.color.succ,
            at.toInstant.toEpochMilli,
            List(
              SlackField("Service", params.serviceName, short = true),
              SlackField("Host", params.taskParams.hostName, short = true),
              SlackField("Total Memory", Megabytes(totalMemory / (1024 * 1024)).toString(Gigabytes), short = true),
              SlackField("Free Memory", Megabytes(freeMemory / (1024 * 1024)).toString(Gigabytes), short = true),
              SlackField("HealthCheck Status", "Good", short = true),
              SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
              SlackField("Time Zone", params.taskParams.zoneId.toString, short = true),
              SlackField("Next check will happen in", fmt.format(params.healthCheck.interval), short = true)
            ) ++ List(
              if (dailySummaries.servicePanic > 0)
                Some(SlackField("Service Panics Today", dailySummaries.servicePanic.toString, short = true))
              else None,
              if (dailySummaries.actionSucc > 0)
                Some(SlackField("Succed Actions Today", dailySummaries.actionSucc.toString, short = true))
              else None,
              if (dailySummaries.actionFail > 0)
                Some(SlackField("Failed Actions Today", dailySummaries.actionFail.toString, short = true))
              else None,
              if (dailySummaries.actionRetries > 0)
                Some(SlackField("Retried Actions Today", dailySummaries.actionRetries.toString, short = true))
              else None
            ).flatten
          ))
      ).asJson.noSpaces
      val ltr = NJLocalTimeRange(params.healthCheck.openTime, params.healthCheck.span, params.taskParams.zoneId)
      service.publish(msg).whenA(ltr.isInBetween(at))

    case ActionRetrying(at, action, params, wdr, error) =>
      val s1 = s"This is the ${toOrdinalWords(wdr.retriesSoFar + 1)} failure of the action, "
      val s2 = s"retry of which takes place in *${fmt.format(wdr.nextDelay)}*, "
      val s3 = s"up to maximum *${params.maxRetries}* retries"
      val msg =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          s1 + s2 + s3,
          List(
            Attachment(
              params.serviceParams.taskParams.color.warn,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Status", "Retrying", short = true),
                SlackField("Took", fmt.format(action.launchTime, at), short = true),
                SlackField("Retry Policy", params.retryPolicy.policy[F].show, short = true),
                SlackField("Action ID", action.id.toString, short = false),
                SlackField(
                  "Cause",
                  StringUtils.abbreviate(error.message, params.serviceParams.maxCauseSize),
                  short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.isShowRetryEvent)

    case ActionFailed(at, action, params, numRetries, notes, error) =>
      val msg =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          notes.value,
          List(
            Attachment(
              params.serviceParams.taskParams.color.fail,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Status", "Failed", short = true),
                SlackField("Took", fmt.format(action.launchTime, at), short = true),
                SlackField("Retries", numRetries.toString, short = true),
                SlackField("Retry Policy", params.retryPolicy.policy[F].show, short = true),
                SlackField("Action ID", action.id.toString, short = false),
                SlackField(
                  "Cause",
                  StringUtils.abbreviate(error.message, params.serviceParams.maxCauseSize),
                  short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.alertMask.alertFail)

    case ActionSucced(at, action, params, numRetries, notes) =>
      val msg =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          notes.value,
          List(
            Attachment(
              params.serviceParams.taskParams.color.succ,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Status", "Completed", short = true),
                SlackField("Took", fmt.format(action.launchTime, at), short = true),
                SlackField("Retries", s"$numRetries/${params.maxRetries}", short = true),
                SlackField("Action ID", action.id.toString, short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.alertMask.alertSucc)

    case ActionQuasiSucced(at, action, params, numSucc, succNotes, failNotes, errors) =>
      val msg: SlackNotification = {
        if (errors.isEmpty)
          SlackNotification(
            params.serviceParams.taskParams.appName,
            succNotes.value,
            List(
              Attachment(
                params.serviceParams.taskParams.color.succ,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", action.actionName, short = true),
                  SlackField("Status", "Completed", short = true),
                  SlackField("Succed", numSucc.toString, short = true),
                  SlackField("Failed", errors.size.toString, short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Action ID", action.id.toString, short = false)
                )
              ))
          )
        else
          SlackNotification(
            params.serviceParams.taskParams.appName,
            failNotes.value,
            List(
              Attachment(
                params.serviceParams.taskParams.color.warn,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", action.actionName, short = true),
                  SlackField("Status", "Quasi Success", short = true),
                  SlackField("Succed", numSucc.toString, short = true),
                  SlackField("Failed", errors.size.toString, short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Action ID", action.id.toString, short = false)
                )
              ))
          )
      }
      service
        .publish(msg.asJson.noSpaces)
        .whenA((params.alertMask.alertSucc && errors.isEmpty) || (params.alertMask.alertFail && errors.nonEmpty))

    case ForYourInformation(_, message) => service.publish(message).void

    // no op
    case _: PassThrough                => F.unit
    case _: ServiceDailySummariesReset => F.unit
  }
}

object SlackService {

  def apply[F[_]: Sync](topic: SnsArn, region: Regions, fmt: DurationFormatter): AlertService[F] =
    new SlackService[F](SimpleNotificationService(topic, region), fmt)

  def apply[F[_]: Sync](topic: SnsArn): AlertService[F] =
    new SlackService[F](SimpleNotificationService(topic), DurationFormatter.default)

  def apply[F[_]: Sync](service: SimpleNotificationService[F]): AlertService[F] =
    new SlackService[F](service, DurationFormatter.default)

}
