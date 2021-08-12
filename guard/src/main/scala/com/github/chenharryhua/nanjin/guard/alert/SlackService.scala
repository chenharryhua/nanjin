package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.Severity
import io.chrisdavenport.cats.time.instances.zoneid
import io.circe.generic.auto.*
import io.circe.syntax.*
import monocle.macros.Lenses
import org.apache.commons.lang3.StringUtils

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

@Lenses final case class SlackAlertMask private (
  alertSucc: Boolean,
  alertRetry: Boolean, // alert every retry
  alertFirstRetry: Boolean, // alert first time failure of the action
  alertStart: Boolean,
  maxCauseSize: Int) // alert action start

final class SlackService[F[_]](service: SimpleNotificationService[F], fmt: DurationFormatter, mask: SlackAlertMask)(
  implicit F: Sync[F])
    extends AlertService[F] with zoneid {

  def withFormatter(fmt: DurationFormatter): SlackService[F]    = new SlackService[F](service, fmt, mask)
  private def updateMask(mask: SlackAlertMask): SlackService[F] = new SlackService[F](service, fmt, mask)

  def showSucc: SlackService[F]                     = updateMask(SlackAlertMask.alertSucc.set(true)(mask))
  def showRetry: SlackService[F]                    = updateMask(SlackAlertMask.alertRetry.set(true)(mask))
  def showFirstRetry: SlackService[F]               = updateMask(SlackAlertMask.alertFirstRetry.set(true)(mask))
  def showStart: SlackService[F]                    = updateMask(SlackAlertMask.alertStart.set(true)(mask))
  def withMaxCauseSize(value: Int): SlackService[F] = updateMask(SlackAlertMask.maxCauseSize.set(value)(mask))

  private val good_color  = "good"
  private val warn_color  = "#ffd79a"
  private val info_color  = "#b3d1ff"
  private val error_color = "danger"

  @SuppressWarnings(Array("ListSize"))
  override def alert(event: NJEvent): F[Unit] = event match {

    case ServiceStarted(at, _, params) =>
      def msg: String = SlackNotification(
        params.taskParams.appName,
        s":rocket: ${params.brief}",
        List(
          Attachment(
            info_color,
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

    case ServicePanic(at, si, params, details, error) =>
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
              error_color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Status", "Restarting", short = true),
                SlackField("Up Time", fmt.format(si.launchTime, at), short = true),
                SlackField("Restarted so far", details.retriesSoFar.show, short = true),
                SlackField("Cumulative Delay", fmt.format(details.cumulativeDelay), short = true),
                SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                SlackField("Cause", StringUtils.abbreviate(error.message, mask.maxCauseSize), short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).void

    case ServiceStopped(at, si, params) =>
      def msg: String =
        SlackNotification(
          params.taskParams.appName,
          ":octagonal_sign: The service was stopped.",
          List(
            Attachment(
              info_color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", fmt.format(si.launchTime, at), short = true),
                SlackField("Status", "Stopped", short = true)
              )
            ))
        ).asJson.noSpaces

      service.publish(msg).void

    case ServiceHealthCheck(at, si, params, dailySummaries) =>
      def msg: String = SlackNotification(
        params.taskParams.appName,
        s":gottarun: *Health Check* \n${StringUtils.abbreviate(dailySummaries.value, mask.maxCauseSize)}",
        List(
          Attachment(
            info_color,
            at.toInstant.toEpochMilli,
            List(
              SlackField("Service", params.serviceName, short = true),
              SlackField("Host", params.taskParams.hostName, short = true),
              SlackField("Up Time", fmt.format(si.launchTime, at), short = true),
              SlackField("Next Check in", fmt.format(params.healthCheckInterval), short = true),
              SlackField("Brief", params.brief, short = false)
            )
          ))
      ).asJson.noSpaces

      service.publish(msg).void

    case ServiceDailySummariesReset(at, si, params, dailySummaries) =>
      def msg: String =
        SlackNotification(
          params.taskParams.appName,
          s":checklist: *Daily Summaries* \n${dailySummaries.value}",
          List(
            Attachment(
              info_color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", fmt.format(si.launchTime, at), short = true),
                SlackField("Brief", params.brief, short = false)
              )
            ))
        ).asJson.noSpaces

      service.publish(msg).void

    case ActionStart(at, _, action, params) =>
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          s"Start running action: *${action.actionName}*",
          List(
            Attachment(
              info_color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action ID", action.id.show, short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(mask.alertStart)

    case ActionRetrying(at, action, params, wdr, error) =>
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          s"This is the *${toOrdinalWords(
            wdr.retriesSoFar + 1)}* failure of the action, retry of which takes place in *${fmt.format(wdr.nextDelay)}*",
          List(
            Attachment(
              warn_color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceParams.serviceName, short = true),
                SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                SlackField("Action", action.actionName, short = true),
                SlackField("Severity", error.severity.entryName, short = true),
                SlackField("Took", fmt.format(action.launchTime, at), short = true),
                SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                SlackField("Action ID", action.id.show, short = false),
                SlackField("Cause", StringUtils.abbreviate(error.message, mask.maxCauseSize), short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).whenA(mask.alertRetry || (mask.alertFirstRetry && wdr.retriesSoFar == 0))

    case ActionFailed(at, action, params, numRetries, notes, error) =>
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          notes.value,
          List(
            Attachment(
              if (error.severity.value === Severity.Notice.value) warn_color else error_color,
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
                SlackField("Cause", StringUtils.abbreviate(error.message, mask.maxCauseSize), short = false)
              )
            ))
        ).asJson.noSpaces
      service.publish(msg).void

    case ActionSucced(at, _, action, params, numRetries, notes) =>
      def msg: String =
        SlackNotification(
          params.serviceParams.taskParams.appName,
          notes.value,
          List(
            Attachment(
              good_color,
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
      service.publish(msg).whenA(mask.alertSucc)

    case ActionQuasiSucced(at, _, action, params, runMode, numSucc, succNotes, failNotes, errors) =>
      def msg: SlackNotification =
        if (errors.isEmpty)
          SlackNotification(
            params.serviceParams.taskParams.appName,
            succNotes.value,
            List(
              Attachment(
                good_color,
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
                warn_color,
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

      service.publish(msg.asJson.noSpaces).whenA(mask.alertSucc)

    case ForYourInformation(_, message) => service.publish(message).void

    // no op
    case _: PassThrough => F.unit

  }
}

object SlackService {

  def apply[F[_]: Sync](service: SimpleNotificationService[F]): SlackService[F] =
    new SlackService[F](
      service,
      DurationFormatter.defaultFormatter,
      SlackAlertMask(
        alertSucc = false,
        alertRetry = false,
        alertFirstRetry = false,
        alertStart = false,
        maxCauseSize = 500))

  def apply[F[_]: Sync](service: Resource[F, SimpleNotificationService[F]]): Resource[F, SlackService[F]] =
    service.map(apply[F])

  def apply[F[_]: Sync](topic: SnsArn): Resource[F, SlackService[F]] =
    apply[F](SimpleNotificationService(topic))

}
