package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import io.chrisdavenport.cats.time.instances.zoneid
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.lang3.StringUtils

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final private class SlackService[F[_]](service: SimpleNotificationService[F], fmt: DurationFormatter)(implicit
  F: Sync[F])
    extends AlertService[F] with zoneid {

  override def alert(event: NJEvent): F[Unit] = event match {

    case ServiceStarted(at, _, params) =>
      val msg = SlackNotification(
        params.taskParams.appName,
        s":rocket: ${params.notes}",
        List(
          Attachment(
            params.taskParams.color.info,
            at.toInstant.toEpochMilli,
            List(
              SlackField("Service", params.serviceName, short = true),
              SlackField("Host", params.taskParams.hostName, short = true),
              SlackField("Status", "(Re)Started", short = true),
              SlackField("Time Zone", params.taskParams.zoneId.show, short = true)
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
          s""":x: The service experienced a panic, $upcoming
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
                SlackField("Restarted so far", details.retriesSoFar.show, short = true),
                SlackField("Cumulative Delay", fmt.format(details.cumulativeDelay), short = true),
                SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                SlackField("Cause", StringUtils.abbreviate(error.message, params.maxCauseSize), short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).void

    case ServiceStopped(at, info, params) =>
      val msg =
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

      service.publish(msg.asJson.noSpaces).void

    case ServiceHealthCheck(at, info, params, dailySummaries) =>
      val msg = SlackNotification(
        params.taskParams.appName,
        s":gottarun: ${StringUtils.abbreviate(dailySummaries.value, params.maxCauseSize)}",
        List(
          Attachment(
            params.taskParams.color.info,
            at.toInstant.toEpochMilli,
            List(
              SlackField("Service", params.serviceName, short = true),
              SlackField("Host", params.taskParams.hostName, short = true),
              SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
              SlackField("HealthCheck Status", "Good", short = true),
              SlackField("Next Check in", fmt.format(params.healthCheck.interval), short = true)
            )
          ))
      ).asJson.noSpaces
      val ltr = NJLocalTimeRange(params.healthCheck.openTime, params.healthCheck.span, params.taskParams.zoneId)
      service.publish(msg).whenA(ltr.isInBetween(at))

    case ServiceDailySummariesReset(at, serviceInfo, params, dailySummaries) =>
      val msg =
        SlackNotification(
          params.taskParams.appName,
          StringUtils.abbreviate(dailySummaries.value, params.maxCauseSize),
          List(
            Attachment(
              params.taskParams.color.info,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", fmt.format(serviceInfo.launchTime, at), short = true)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.taskParams.dailySummaryReset.enabled)

    case ActionStart(at, action, params) =>
      val msg =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          "",
          List(
            Attachment(
              params.serviceParams.taskParams.color.info,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Status", "Start", short = true),
                SlackField("Action ID", action.id.show, short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.alertMask.alertStart)

    case ActionRetrying(at, action, params, wdr, error) =>
      val s1 = s"This is the *${toOrdinalWords(wdr.retriesSoFar + 1)}* failure of the action, "
      val s2 = s"retry of which takes place in *${fmt.format(wdr.nextDelay)}*"
      val msg =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          s1 + s2,
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
                SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                SlackField("Action ID", action.id.show, short = false),
                SlackField(
                  "Cause",
                  StringUtils.abbreviate(error.message, params.serviceParams.maxCauseSize),
                  short = false)
              )
            ))
        ).asJson.noSpaces
      service
        .publish(msg)
        .whenA(params.alertMask.alertRetry || (params.alertMask.alertFirstRetry && wdr.retriesSoFar == 0))

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
                SlackField("Retried", numRetries.show, short = true),
                SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                SlackField("Action ID", action.id.show, short = false),
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
                SlackField("Retried", s"$numRetries/${params.retry.maxRetries}", short = true),
                SlackField("Action ID", action.id.show, short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.alertMask.alertSucc)

    case ActionQuasiSucced(at, action, params, runMode, numSucc, succNotes, failNotes, errors) =>
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
                  SlackField("Succed", numSucc.show, short = true),
                  SlackField("Failed", errors.size.show, short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Run Mode", runMode.show, short = true),
                  SlackField("Action ID", action.id.show, short = false)
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
                  SlackField("Succed", numSucc.show, short = true),
                  SlackField("Failed", errors.size.show, short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Run Mode", runMode.show, short = true),
                  SlackField("Action ID", action.id.show, short = false)
                )
              ))
          )
      }
      service
        .publish(msg.asJson.noSpaces)
        .whenA((params.alertMask.alertSucc && errors.isEmpty) || (params.alertMask.alertFail && errors.nonEmpty))

    case ForYourInformation(_, message, _) => service.publish(message).void

    // no op
    case _: PassThrough => F.unit

  }
}

object SlackService {

  def apply[F[_]: Sync](topic: SnsArn, region: Regions, fmt: DurationFormatter): Resource[F, AlertService[F]] =
    SimpleNotificationService(topic, region).map(s => new SlackService[F](s, fmt))

  def apply[F[_]: Sync](service: SimpleNotificationService[F]): AlertService[F] =
    new SlackService[F](service, DurationFormatter.defaultFormatter)

  def apply[F[_]: Sync](service: Resource[F, SimpleNotificationService[F]]): Resource[F, AlertService[F]] =
    service.map(apply[F])

  def apply[F[_]: Sync](topic: SnsArn): Resource[F, AlertService[F]] =
    apply[F](SimpleNotificationService(topic))

}
