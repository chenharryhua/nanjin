package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.kernel.Order
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.{Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{Pipe, Stream}
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.{localdatetime, localtime, zoneddatetime, zoneid}

import java.text.NumberFormat
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration

object slack {
  def apply[F[_]: Async](snsResource: Resource[F, SimpleNotificationService[F]]): SlackPipe[F] =
    new SlackPipe[F](
      snsResource,
      SlackConfig[F](
        goodColor = "good",
        warnColor = "#ffd79a",
        infoColor = "#b3d1ff",
        errorColor = "danger",
        maxTextSize = 500,
        durationFormatter = DurationFormatter.defaultFormatter,
        reportInterval = None,
        isShowRetry = true,
        extraSlackFields = Async[F].pure(Nil)
      )
    )

  def apply[F[_]: Async](snsArn: SnsArn): SlackPipe[F] = apply[F](SimpleNotificationService[F](snsArn))
}

final private case class SlackConfig[F[_]](
  goodColor: String,
  warnColor: String,
  infoColor: String,
  errorColor: String,
  maxTextSize: Int,
  durationFormatter: DurationFormatter,
  reportInterval: Option[FiniteDuration],
  isShowRetry: Boolean,
  extraSlackFields: F[List[SlackField]]
)

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final class SlackPipe[F[_]] private[observers] (
  snsResource: Resource[F, SimpleNotificationService[F]],
  cfg: SlackConfig[F])(implicit F: Async[F])
    extends Pipe[F, NJEvent, NJEvent] with zoneid with localdatetime with localtime with zoneddatetime {

  private def updateSlackConfig(f: SlackConfig[F] => SlackConfig[F]): SlackPipe[F] =
    new SlackPipe[F](snsResource, f(cfg))

  def withGoodColor(color: String): SlackPipe[F]                  = updateSlackConfig(_.copy(goodColor = color))
  def withWarnColor(color: String): SlackPipe[F]                  = updateSlackConfig(_.copy(warnColor = color))
  def withInfoColor(color: String): SlackPipe[F]                  = updateSlackConfig(_.copy(infoColor = color))
  def withErrorColor(color: String): SlackPipe[F]                 = updateSlackConfig(_.copy(errorColor = color))
  def withMaxTextSize(size: Int): SlackPipe[F]                    = updateSlackConfig(_.copy(maxTextSize = size))
  def withDurationFormatter(fmt: DurationFormatter): SlackPipe[F] = updateSlackConfig(_.copy(durationFormatter = fmt))
  def withoutRetry: SlackPipe[F]                                  = updateSlackConfig(_.copy(isShowRetry = false))

  def withSlackField(title: String, value: F[String], isShort: Boolean): SlackPipe[F] =
    updateSlackConfig(_.copy(extraSlackFields = for {
      esf <- cfg.extraSlackFields
      v <- value
    } yield esf :+ SlackField(title, v, isShort)))

  def withSlackField(title: String, value: String, isShort: Boolean): SlackPipe[F] =
    withSlackField(title, F.pure(value), isShort)

  /** one metrics report allowed to show in the interval
    */
  def withReportInterval(interval: FiniteDuration): SlackPipe[F] =
    updateSlackConfig(_.copy(reportInterval = Some(interval)))

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    for {
      sns <- Stream.resource(snsResource)
      ref <- Stream.eval(F.ref[Map[ServiceParams, ZonedDateTime]](Map.empty))
      event <- es.evalMap { e =>
        val updateRef: F[Unit] = e match {
          case ServiceStarted(_, serviceInfo, serviceParams) =>
            ref.update(_.updated(serviceParams, serviceInfo.launchTime))
          case ServiceStopped(_, _, serviceParams, _) => ref.update(_.removed(serviceParams))
          case _                                      => F.unit
        }
        updateRef >> publish(e, sns).attempt.as(e)
      }.onFinalize { // publish good bye message to slack
        for {
          ts <- F.realTimeInstant
          extra <- cfg.extraSlackFields
          services <- ref.get
          msg = SlackNotification(
            "Service Termination Notice",
            ":octagonal_sign: *Following service(s) was terminated*",
            services.toList.map(ss =>
              Attachment(
                cfg.warnColor,
                ss._2.toInstant.toEpochMilli,
                List(
                  SlackField("Service", ss._1.uniqueName, short = true),
                  SlackField("Host", ss._1.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(ss._2.toInstant, ts), short = true),
                  SlackField("Status", "Terminated", short = true),
                  SlackField("Task", ss._1.taskParams.appName, short = true)
                )
              )) ::: (if (extra.isEmpty) Nil
                      else
                        List(
                          Attachment(
                            cfg.infoColor,
                            ts.toEpochMilli,
                            extra
                          )))
          )
          _ <- sns.publish(msg.asJson.noSpaces).attempt.void
        } yield ()
      }
    } yield event

  private def toOrdinalWords(n: Long): String = {
    val w =
      if (n % 100 / 10 == 1) "th"
      else {
        n % 10 match {
          case 1 => "st"
          case 2 => "nd"
          case 3 => "rd"
          case _ => "th"
        }
      }
    s"$n$w"
  }

  private def toText(counters: Map[String, Long]): String = {
    val fmt: NumberFormat = NumberFormat.getIntegerInstance
    counters.map(x => s"${x._1}: *${fmt.format(x._2)}*").toList.sorted.mkString("\n")
  }

  @SuppressWarnings(Array("ListSize"))
  private def publish(event: NJEvent, sns: SimpleNotificationService[F]): F[Unit] =
    event match {

      case ServiceStarted(at, si, params) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.taskParams.appName,
            s":rocket:",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.uniqueName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField("Status", "(Re)Started", short = true),
                  SlackField("Time Zone", params.taskParams.zoneId.show, short = true)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ServicePanic(at, si, params, details, error) =>
        def upcoming: String = details.upcomingDelay.map(cfg.durationFormatter.format) match {
          case None     => "should never see this" // never happen
          case Some(ts) => s"restart of which takes place in *$ts* meanwhile the service is dysfunctional."
        }
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.taskParams.appName,
            s""":alarm: The service experienced a panic, $upcoming
               |Search *${error.uuid}* in log file to find full exception.""".stripMargin,
            List(
              Attachment(
                cfg.errorColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.uniqueName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField("Status", "Restarting", short = true),
                  SlackField("Restart Count", details.retriesSoFar.show, short = true),
                  SlackField("Cumulative Delay", cfg.durationFormatter.format(details.cumulativeDelay), short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, cfg.maxTextSize), short = false)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ServiceAlert(at, _, params, alertName, message) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.taskParams.appName,
            StringUtils.abbreviate(message, cfg.maxTextSize),
            List(
              Attachment(
                cfg.warnColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.uniqueName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Alert", alertName, short = true)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ServiceStopped(at, si, params, snapshot) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.taskParams.appName,
            s":octagonal_sign: The service was stopped. performed:\n${toText(snapshot.counters)}",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.uniqueName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField("Status", "Stopped", short = true)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case MetricsReport(index, at, si, params, snapshot) =>
        val msg = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.taskParams.appName,
            s":gottarun: ${StringUtils.abbreviate(toText(snapshot.counters), cfg.maxTextSize)}",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.uniqueName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField(
                    s"Next",
                    params.metric
                      .next(at, cfg.reportInterval, si.launchTime)
                      .fold("no report thereafter")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
                    short = true
                  )
                ) ::: extra
              ))
          ))

        msg
          .flatMap(m => sns.publish(m.asJson.noSpaces))
          .whenA(params.metric.isShow(at, cfg.reportInterval, si.launchTime) || index === 1L)

      case MetricsReset(at, si, params, prev, next, snapshot) =>
        val toNow =
          prev.map(p => cfg.durationFormatter.format(Order.max(p, si.launchTime), at)).fold("")(dur => s" in past $dur")
        val summaries = s"*This is a summary of activities performed by the service$toNow*"

        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.taskParams.appName,
            s"$summaries\n${toText(snapshot.counters)}",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.uniqueName, short = true),
                  SlackField("Host", params.taskParams.hostName, short = true),
                  SlackField("Up Time", cfg.durationFormatter.format(si.launchTime, at), short = true),
                  SlackField(
                    s"Next Metrics Reset",
                    next.fold("no time")(_.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show),
                    short = true
                  )
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ActionStart(params, action, at) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.serviceParams.taskParams.appName,
            s"Kick off action: *${params.actionName}*",
            List(
              Attachment(
                cfg.infoColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.uniqueName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action ID", action.display, short = true),
                  SlackField("Status", "Started", short = true)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ActionRetrying(params, action, at, wdr, error) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.serviceParams.taskParams.appName,
            s"This is the *${toOrdinalWords(wdr.retriesSoFar + 1L)}* failure of the action, retry of which takes place in *${cfg.durationFormatter
              .format(wdr.nextDelay)}*",
            List(
              Attachment(
                cfg.warnColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.uniqueName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Action ID", action.display, short = true),
                  SlackField("Status", "Retrying", short = true),
                  SlackField("Hitherto", cfg.durationFormatter.format(action.launchTime, at), short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, cfg.maxTextSize), short = false)
                ) ::: extra
              ))
          ))

        msg
          .flatMap(m => sns.publish(m.asJson.noSpaces))
          .whenA(params.importance.value > Importance.Low.value && cfg.isShowRetry)

      case ActionFailed(params, action, at, numRetries, notes, error) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.serviceParams.taskParams.appName,
            StringUtils.abbreviate(notes.value, cfg.maxTextSize),
            List(
              Attachment(
                cfg.errorColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.uniqueName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Action ID", action.display, short = true),
                  SlackField("Status", "Failed", short = true),
                  SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                  SlackField("Importance", params.importance.show, short = true),
                  SlackField("Retries", numRetries.show, short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, cfg.maxTextSize), short = false)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).whenA(params.importance.value > Importance.Low.value)

      case ActionSucced(params, action, at, numRetries, notes) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          SlackNotification(
            params.serviceParams.taskParams.appName,
            StringUtils.abbreviate(notes.value, cfg.maxTextSize),
            List(
              Attachment(
                cfg.goodColor,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.uniqueName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Action ID", action.display, short = true),
                  SlackField("Status", "Completed", short = true),
                  SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                  SlackField("Retries", s"$numRetries/${params.retry.maxRetries}", short = true)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ActionQuasiSucced(params, action, at, runMode, numSucc, succNotes, failNotes, errors) =>
        val msg: F[SlackNotification] = cfg.extraSlackFields.map(extra =>
          if (errors.isEmpty)
            SlackNotification(
              params.serviceParams.taskParams.appName,
              StringUtils.abbreviate(succNotes.value, cfg.maxTextSize),
              List(
                Attachment(
                  cfg.goodColor,
                  at.toInstant.toEpochMilli,
                  List(
                    SlackField("Service", params.serviceParams.uniqueName, short = true),
                    SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                    SlackField("Action", params.actionName, short = true),
                    SlackField("Action ID", action.display, short = true),
                    SlackField("Status", "Completed", short = true),
                    SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                    SlackField("Succed", numSucc.show, short = true),
                    SlackField("Failed", errors.size.show, short = true),
                    SlackField("Run Mode", runMode.show, short = true)
                  ) ::: extra
                ))
            )
          else
            SlackNotification(
              params.serviceParams.taskParams.appName,
              StringUtils.abbreviate(failNotes.value, cfg.maxTextSize),
              List(
                Attachment(
                  cfg.warnColor,
                  at.toInstant.toEpochMilli,
                  List(
                    SlackField("Service", params.serviceParams.uniqueName, short = true),
                    SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                    SlackField("Action", params.actionName, short = true),
                    SlackField("Action ID", action.display, short = true),
                    SlackField("Status", "Quasi Success", short = true),
                    SlackField("Took", cfg.durationFormatter.format(action.launchTime, at), short = true),
                    SlackField("Succed", numSucc.show, short = true),
                    SlackField("Failed", errors.size.show, short = true),
                    SlackField("Run Mode", runMode.show, short = true)
                  ) ::: extra
                ))
            ))

        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      // no op
      case _: PassThrough => F.unit
    }
}
