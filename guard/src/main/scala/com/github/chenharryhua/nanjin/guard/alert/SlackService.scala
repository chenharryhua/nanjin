package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.amazonaws.regions.Regions
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.Severity
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

  @SuppressWarnings(Array("ListSize"))
  override def alert(event: NJEvent): F[Unit] = event match {

    case ServiceStarted(at, _, params) =>
      def msg: String = SlackNotification(
        params.taskParams.appName,
        s":rocket: ${params.brief}",
        List(
          Attachment(
            Severity.Informational.color,
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
      def upcoming: String = details.upcomingDelay.map(fmt.format) match {
        case None     => "should never see this" // never happen
        case Some(ts) => s"restart of which takes place in *$ts* meanwhile the service is dysfunctional."
      }
      def msg: String =
        SlackNotification(
          params.taskParams.appName,
          s""":x: The service experienced a panic, $upcoming
             |Search *${error.id}* in log file to find full exception.""".stripMargin,
          List(
            Attachment(
              Severity.Critical.color,
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
      def msg: String =
        SlackNotification(
          params.taskParams.appName,
          ":octagonal_sign: The service was stopped.",
          List(
            Attachment(
              Severity.Informational.color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
                SlackField("Status", "Stopped", short = true)
              )
            ))
        ).asJson.noSpaces

      service.publish(msg).void

    case ServiceHealthCheck(at, info, params, dailySummaries) =>
      def msg: String = SlackNotification(
        params.taskParams.appName,
        s":gottarun: *Health Check* \n${StringUtils.abbreviate(dailySummaries.value, params.maxCauseSize)}",
        List(
          Attachment(
            Severity.Informational.color,
            at.toInstant.toEpochMilli,
            List(
              SlackField("Service", params.serviceName, short = true),
              SlackField("Host", params.taskParams.hostName, short = true),
              SlackField("Up Time", fmt.format(info.launchTime, at), short = true),
              SlackField("Next Check in", fmt.format(params.healthCheck.interval), short = true),
              SlackField("Brief", params.brief, short = false)
            )
          ))
      ).asJson.noSpaces
      def ltr = NJLocalTimeRange(params.healthCheck.openTime, params.healthCheck.span, params.taskParams.zoneId)
      service.publish(msg).whenA(ltr.isInBetween(at))

    case ServiceDailySummariesReset(at, serviceInfo, params, dailySummaries) =>
      def msg: String =
        SlackNotification(
          params.taskParams.appName,
          s":checklist: *Daily Summaries* \n${dailySummaries.value}",
          List(
            Attachment(
              Severity.Informational.color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", fmt.format(serviceInfo.launchTime, at), short = true),
                SlackField("Brief", params.brief, short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.taskParams.dailySummaryReset.enabled)

    case ActionStart(at, action, params) =>
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          s"Start running action: *${action.actionName}*",
          List(
            Attachment(
              Severity.Informational.color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action ID", action.id.show, short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(params.alertMask.alertStart)

    case ActionRetrying(at, action, params, wdr, error) =>
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          s"This is the *${toOrdinalWords(
            wdr.retriesSoFar + 1)}* failure of the action, retry of which takes place in *${fmt.format(wdr.nextDelay)}*",
          List(
            Attachment(
              error.severity.color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Severity", error.severity.entryName, short = true),
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
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          notes.value,
          List(
            Attachment(
              error.severity.color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Severity", error.severity.entryName, short = true),
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
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          notes.value,
          List(
            Attachment(
              Severity.Success.color,
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
      def msg: SlackNotification =
        if (errors.isEmpty)
          SlackNotification(
            params.serviceParams.taskParams.appName,
            succNotes.value,
            List(
              Attachment(
                Severity.Success.color,
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
                Severity.Warning.color,
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
