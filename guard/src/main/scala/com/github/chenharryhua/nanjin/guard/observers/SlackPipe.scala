package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{Pipe, Stream}
import io.chrisdavenport.cats.time.instances.{localtime, zoneid}
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.lang3.StringUtils

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import scala.collection.JavaConverters.*
import scala.compat.java8.DurationConverters.{DurationOps, FiniteDurationops}
import scala.concurrent.duration.FiniteDuration

object slack {
  def apply[F[_]: Sync](snsResource: Resource[F, SimpleNotificationService[F]]): SlackPipe[F] =
    new SlackPipe[F](
      snsResource,
      SlackConfig(
        goodColor = "good",
        warnColor = "#ffd79a",
        infoColor = "#b3d1ff",
        errorColor = "danger",
        maxCauseSize = 500,
        durationFormatter = DurationFormatter.defaultFormatter,
        reportInterval = None
      )
    )

  def apply[F[_]: Sync](snsArn: SnsArn): SlackPipe[F] = apply[F](SimpleNotificationService[F](snsArn))
}

final private case class SlackConfig(
  goodColor: String,
  warnColor: String,
  infoColor: String,
  errorColor: String,
  maxCauseSize: Int,
  durationFormatter: DurationFormatter,
  reportInterval: Option[FiniteDuration]
)

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final class SlackPipe[F[_]] private[observers] (
  snsResource: Resource[F, SimpleNotificationService[F]],
  cfg: SlackConfig)(implicit F: Sync[F])
    extends Pipe[F, NJEvent, NJEvent] with zoneid with localtime {

  private def updateSlackConfig(f: SlackConfig => SlackConfig): SlackPipe[F] =
    new SlackPipe[F](snsResource, f(cfg))

  def withGoodColor(color: String): SlackPipe[F]                  = updateSlackConfig(_.copy(goodColor = color))
  def withWarnColor(color: String): SlackPipe[F]                  = updateSlackConfig(_.copy(warnColor = color))
  def withInfoColor(color: String): SlackPipe[F]                  = updateSlackConfig(_.copy(infoColor = color))
  def withErrorColor(color: String): SlackPipe[F]                 = updateSlackConfig(_.copy(errorColor = color))
  def withMaxCauseSize(size: Int): SlackPipe[F]                   = updateSlackConfig(_.copy(maxCauseSize = size))
  def withDurationFormatter(fmt: DurationFormatter): SlackPipe[F] = updateSlackConfig(_.copy(durationFormatter = fmt))

  def withReportInterval(interval: FiniteDuration): SlackPipe[F] =
    updateSlackConfig(_.copy(reportInterval = Some(interval)))

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    Stream.resource(snsResource).flatMap(s => es.evalMap(e => send(e, s).as(e)))

  private def toOrdinalWords(n: Long): String = n + {
    if (n % 100 / 10 == 1) "th"
    else
      n % 10 match {
        case 1 => "st"
        case 2 => "nd"
        case 3 => "rd"
        case _ => "th"
      }
  }

  private def translate(mrw: MetricRegistryWrapper): String =
    mrw.registry.fold("") { mr =>
      val timer   = mr.getTimers.asScala.map { case (s, t) => s"$s: *${t.getCount}*" }.toList
      val counter = mr.getCounters.asScala.map { case (s, c) => s"$s: *${c.getCount}*" }.toList
      (timer ::: counter).sorted.mkString("\n")
    }

  @SuppressWarnings(Array("ListSize"))
  private def send(event: NJEvent, sns: SimpleNotificationService[F]): F[Unit] =
    event match {

      case ServiceStarted(at, _, params) =>
        def msg: String = SlackNotification(
          params.taskParams.appName,
          s":rocket: ${params.brief}",
          List(
            Attachment(
              cfg.infoColor,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Status", "(Re)Started", short = true),
                SlackField("Time Zone", params.taskParams.zoneId.show, short = true)
              )
            ))
        ).asJson.noSpaces
        sns.publish(msg).void

      case ServicePanic(at, si, params, details, error) =>
        def upcoming: String = details.upcomingDelay.map(cfg.durationFormatter.format) match {
          case None     => "should never see this" // never happen
          case Some(ts) => s"restart of which takes place in *$ts* meanwhile the service is dysfunctional."
        }
        def msg: String =
          SlackNotification(
            params.taskParams.appName,
            s""":x: The service experienced a panic, $upcoming
               |Search *${error.uuid}* in log file to find full exception.""".stripMargin,
            List(
              Attachment(
                cfg.errorColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Status", "Restarting", short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField("Restarted so far", details.retriesSoFar.show, short = true),
                  SlackField("Cumulative Delay", cfg.durationFormatter.format(details.cumulativeDelay), short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, cfg.maxCauseSize), short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).void

      case ServiceStopped(at, si, params) =>
        def msg: String =
          SlackNotification(
            params.taskParams.appName,
            ":octagonal_sign: The service was stopped.",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField("Status", "Stopped", short = true)
                )
              ))
          ).asJson.noSpaces

        sns.publish(msg).void

      case MetricsReport(idx, at, si, params, prev, next, metrics) =>
        def msg: String = SlackNotification(
          params.taskParams.appName,
          StringUtils.abbreviate(translate(metrics), cfg.maxCauseSize),
          List(
            Attachment(
              cfg.infoColor,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                SlackField(
                  s"Next(${toOrdinalWords(idx + 1)}) Check at", // https://english.stackexchange.com/questions/182660/on-vs-at-with-date-and-time
                  next.fold("no time")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
                  short = true
                ),
                SlackField("Brief", params.brief, short = false)
              )
            ))
        ).asJson.noSpaces

        // only show events that cross interval border.
        val isShow: Boolean = (prev, cfg.reportInterval).mapN { case (prev, interval) =>
          if (Duration.between(prev, at).toScala >= interval) true
          else {
            val border: ZonedDateTime =
              si.launchTime.plus(((Duration.between(si.launchTime, at).toScala / interval).toLong * interval).toJava)
            if (prev.isBefore(border) && at.isAfter(border)) true else false
          }
        }.fold(true)(identity)

        sns.publish(msg).whenA(isShow)

      case ActionStart(params, action, at) =>
        def msg: String =
          SlackNotification(
            params.serviceParams.taskParams.appName,
            s"Start running action: *${params.actionName}*",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action ID", action.uuid.show, short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).void

      case ActionRetrying(params, action, at, wdr, error) =>
        def msg: String =
          SlackNotification(
            params.serviceParams.taskParams.appName,
            s"This is the *${toOrdinalWords(wdr.retriesSoFar + 1L)}* failure of the action, retry of which takes place in *${cfg.durationFormatter
              .format(wdr.nextDelay)}*",
            List(
              Attachment(
                cfg.warnColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Action ID", action.uuid.show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, cfg.maxCauseSize), short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).whenA(params.importance.value > Importance.Low.value)

      case ActionFailed(params, action, at, numRetries, notes, error) =>
        def msg: String =
          SlackNotification(
            params.serviceParams.taskParams.appName,
            notes.value,
            List(
              Attachment(
                if (params.importance.value > Importance.Low.value) cfg.errorColor else cfg.warnColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Importance", params.importance.show, short = true),
                  SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                  SlackField("Retried", numRetries.show, short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Action ID", action.uuid.show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, cfg.maxCauseSize), short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).whenA(params.importance.value > Importance.Low.value)

      case ActionSucced(params, action, at, numRetries, notes) =>
        def msg: String =
          SlackNotification(
            params.serviceParams.taskParams.appName,
            notes.value,
            List(
              Attachment(
                cfg.goodColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Status", "Completed", short = true),
                  SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                  SlackField("Retried", s"$numRetries/${params.retry.maxRetries}", short = true),
                  SlackField("Action ID", action.uuid.show, short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).void

      case ActionQuasiSucced(params, action, at, runMode, numSucc, succNotes, failNotes, errors) =>
        def msg: SlackNotification =
          if (errors.isEmpty)
            SlackNotification(
              params.serviceParams.taskParams.appName,
              succNotes.value,
              List(
                Attachment(
                  cfg.goodColor,
                  at.toInstant.toEpochMilli,
                  List(
                    SlackField("Service", params.serviceParams.serviceName, short = true),
                    SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                    SlackField("Action", params.actionName, short = true),
                    SlackField("Status", "Completed", short = true),
                    SlackField("Succed", numSucc.show, short = true),
                    SlackField("Failed", errors.size.show, short = true),
                    SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                    SlackField("Run Mode", runMode.show, short = true),
                    SlackField("Action ID", action.uuid.show, short = false)
                  )
                ))
            )
          else
            SlackNotification(
              params.serviceParams.taskParams.appName,
              failNotes.value,
              List(
                Attachment(
                  cfg.warnColor,
                  at.toInstant.toEpochMilli,
                  List(
                    SlackField("Service", params.serviceParams.serviceName, short = true),
                    SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                    SlackField("Action", params.actionName, short = true),
                    SlackField("Status", "Quasi Success", short = true),
                    SlackField("Succed", numSucc.show, short = true),
                    SlackField("Failed", errors.size.show, short = true),
                    SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                    SlackField("Run Mode", runMode.show, short = true),
                    SlackField("Action ID", action.uuid.show, short = false)
                  )
                ))
            )

        sns.publish(msg.asJson.noSpaces).void

      // no op
      case _: PassThrough => F.unit
    }

}
