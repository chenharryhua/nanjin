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
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.{localdatetime, localtime, zoneddatetime, zoneid}

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration

object slack {
  def apply[F[_]: Async](snsResource: Resource[F, SimpleNotificationService[F]]): SlackPipe[F] =
    new SlackPipe[F](
      snsResource,
      SlackConfig[F](
        goodColor = "#36a64f",
        warnColor = "#ffd79a",
        infoColor = "#b3d1ff",
        errorColor = "#935252",
        durationFormatter = DurationFormatter.defaultFormatter,
        reportInterval = None,
        isShowRetry = true,
        extraSlackSections = Async[F].pure(Nil)
      )
    )

  def apply[F[_]: Async](snsArn: SnsArn): SlackPipe[F] = apply[F](SimpleNotificationService[F](snsArn))
}

final private case class SlackConfig[F[_]](
  goodColor: String,
  warnColor: String,
  infoColor: String,
  errorColor: String,
  durationFormatter: DurationFormatter,
  reportInterval: Option[FiniteDuration],
  isShowRetry: Boolean,
  extraSlackSections: F[List[Section]]
)

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class Section(text: String)
private object Section {
  implicit val SectionEncoder: Encoder[Section] = (section: Section) =>
    Json.obj(
      ("type", "section".asJson),
      ("text", Json.obj(("type", "mrkdwn".asJson), ("text", section.text.asJson)))
    )
}
final private case class Attachments(color: String, blocks: List[Section])
final private case class SlackApp(username: String, attachments: List[Attachments])

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
  def withDurationFormatter(fmt: DurationFormatter): SlackPipe[F] = updateSlackConfig(_.copy(durationFormatter = fmt))
  def withoutRetry: SlackPipe[F]                                  = updateSlackConfig(_.copy(isShowRetry = false))

  def withSection(value: F[String]): SlackPipe[F] =
    updateSlackConfig(_.copy(extraSlackSections = for {
      esf <- cfg.extraSlackSections
      v <- value
    } yield esf :+ Section(abbreviate(v))))

  def withSection(value: String): SlackPipe[F] = withSection(F.pure(value))

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
          extra <- cfg.extraSlackSections
          services <- ref.get
          msg = SlackApp(
            username = "Service Termination Notice",
            attachments = List(
              Attachments(
                color = cfg.warnColor,
                blocks = services.toList.map(ss =>
                  Section(s"""|:octagonal_sign: Terminated Service
                              |*App:* ${ss._1.taskParams.appName}
                              |*Up Time:* ${cfg.durationFormatter
                    .format(ss._2.toInstant, ts)}
                              |*Service:* ${ss._1.uniqueName}
                              |*Host:* ${ss._1.taskParams.hostName}""".stripMargin))
              ))
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

  // slack not allow message larger than 3000 chars
  private def abbreviate(msg: String): String = StringUtils.abbreviate(msg, 2950)

  private def publish(event: NJEvent, sns: SimpleNotificationService[F]): F[Unit] =
    event match {
      case ServiceStarted(at, si, params) =>
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.infoColor,
                blocks = List(
                  Section(s"""|:rocket: (Re)Started Service
                              |*Up Time:* ${cfg.durationFormatter.format(si.launchTime, at)}
                              |*Time Zone:* ${params.taskParams.zoneId.show}""".stripMargin),
                  Section(s"""|*Service:* ${params.uniqueName}
                              |*Host:* ${params.taskParams.hostName}""".stripMargin)
                )
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ServicePanic(at, si, params, details, error) =>
        val upcoming: String = details.upcomingDelay.map(cfg.durationFormatter.format) match {
          case None     => "report to developer once you see this message" // never happen
          case Some(ts) => s"restart of which takes place in *$ts* meanwhile the service is dysfunctional."
        }
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.errorColor,
                blocks = List(
                  Section(s"""|:alarm: Service Panic 
                              |The service experienced a panic, $upcoming""".stripMargin),
                  Section(s"""|*Up Time:* ${cfg.durationFormatter.format(si.launchTime, at)} 
                              |*Restarts:* ${details.retriesSoFar.show}
                              |*Cumulative Delay:* ${cfg.durationFormatter.format(details.cumulativeDelay)} 
                              |*Retry Policy:* ${params.retry.policy[F].show}""".stripMargin),
                  Section(s"""|*Service:* ${params.uniqueName}
                              |*Host:* ${params.taskParams.hostName}""".stripMargin)
                ) ::: extra
              ),
              Attachments(
                color = cfg.errorColor,
                blocks = List(Section(s"*Cause*:\n```${abbreviate(error.stackTrace)}```"))
              )
            )
          ))

        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ServiceAlert(at, si, params, alertName, message) =>
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.warnColor,
                blocks = List(
                  Section(s"""|*Alert:* ${alertName}
                              |*Up Time:* ${cfg.durationFormatter.format(si.launchTime, at)}
                              |""".stripMargin),
                  Section(s"""|*Service:* ${params.uniqueName}
                              |*Host:* ${params.taskParams.hostName}""".stripMargin),
                  Section(abbreviate(message))
                )
              )
            )
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ServiceStopped(at, si, params, snapshot) =>
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.warnColor,
                blocks = List(
                  Section(s"""|:octagonal_sign: Service Stopped.
                              |*Up Time:* ${cfg.durationFormatter.format(si.launchTime, at)}""".stripMargin),
                  Section(s"""|*Service:* ${params.uniqueName}
                              |*Host:* ${params.taskParams.hostName}""".stripMargin)
                ) ::: extra
              ))
          ))
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case MetricsReport(index, at, si, params, snapshot) =>
        val msg = cfg.extraSlackSections.map { extra =>
          val next = nextTime(params.metric.reportSchedule, at, cfg.reportInterval, si.launchTime)
            .fold("no report thereafter")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)

          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.infoColor,
                blocks = List(
                  Section(s"""|:gottarun: Health Check 
                              |*Next Time:* $next
                              |*Up Time:* ${cfg.durationFormatter.format(si.launchTime, at)}""".stripMargin),
                  Section(s"""|*Service:* ${params.uniqueName}
                              |*Host:* ${params.taskParams.hostName}""".stripMargin)
                ) ::: extra
              ),
              Attachments(color = cfg.infoColor, blocks = List(Section(abbreviate(snapshot.show))))
            )
          )
        }

        msg
          .flatMap(m => sns.publish(m.asJson.noSpaces))
          .whenA(isShowMetrics(params.metric.reportSchedule, at, cfg.reportInterval, si.launchTime) || index === 1L)

      case MetricsReset(at, si, params, prev, next, snapshot) =>
        val toNow =
          prev.map(p => cfg.durationFormatter.format(Order.max(p, si.launchTime), at)).fold("")(dur => s" in past $dur")
        val summaries = s"*This is a summary of activities performed by the service$toNow*"

        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.infoColor,
                blocks = List(
                  Section(summaries),
                  Section(
                    s"""|*Next Reset:* ${next.fold("no time")(_.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show)}
                        |*Up Time:* ${cfg.durationFormatter.format(si.launchTime, at)}""".stripMargin),
                  Section(s"""|*Service:* ${params.uniqueName}
                              |*Host:* ${params.taskParams.hostName}""".stripMargin)
                ) ::: extra
              ),
              Attachments(color = cfg.infoColor, blocks = List(Section(abbreviate(snapshot.show))))
            )
          )
        }
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).void

      case ActionStart(params, action) =>
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.infoColor,
                blocks = List(
                  Section(s"""|Kick off action: *${params.uniqueName}*""".stripMargin),
                  Section(s"""|*ActionID:* ${action.uuid.show}""".stripMargin),
                  Section(s"""|*Service:* ${params.serviceParams.uniqueName}
                              |*Host:* ${params.serviceParams.taskParams.hostName}""".stripMargin)
                )
              ))
          )
        }
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).whenA(params.importance === Importance.Critical)

      case ActionRetrying(params, action, at, wdr, error) =>
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.warnColor,
                blocks = List(
                  Section(
                    s"This is the *${toOrdinalWords(wdr.retriesSoFar + 1L)}* failure of the action *${params.uniqueName}*, retry of which takes place in *${cfg.durationFormatter
                      .format(wdr.nextDelay)}*"),
                  Section(s"""|*ActionID:* ${action.uuid.show}
                              |*So Far Took:* ${cfg.durationFormatter.format(action.launchTime, at)}
                              |*Retry Policy:* ${params.retry.policy[F].show}""".stripMargin),
                  Section(s"""|*Service:* ${params.serviceParams.uniqueName}
                              |*Host:* ${params.serviceParams.taskParams.hostName}""".stripMargin),
                  Section(s"*Cause:* \n```${abbreviate(error.stackTrace)}```")
                )
              ))
          )
        }
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).whenA(params.importance =!= Importance.Low && cfg.isShowRetry)

      case ActionFailed(params, action, at, numRetries, notes, error) =>
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.errorColor,
                blocks = List(
                  Section(s"The action *${params.uniqueName}* was failed after *${numRetries.show}* retries"),
                  Section(s"""|*ActionID:* ${action.uuid.show}
                              |*ErrorID:* ${error.uuid.show}
                              |*Took:* ${cfg.durationFormatter.format(action.launchTime, at)}
                              |*Retry Policy:* ${params.retry.policy[F].show}
                              |""".stripMargin),
                  Section(s"""|*Service:* ${params.serviceParams.uniqueName}
                              |*Host:* ${params.serviceParams.taskParams.hostName}""".stripMargin),
                  Section(s"*Cause:* \n```${abbreviate(error.stackTrace)}```")
                ) ::: (if (notes.value.isEmpty) Nil else List(Section(abbreviate(notes.value))))
              )
            )
          )
        }
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).whenA(params.importance =!= Importance.Low)

      case ActionSucced(params, action, at, numRetries, notes) =>
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.goodColor,
                blocks = List(
                  Section(s"The action *${params.uniqueName}* was successfully completed in *${cfg.durationFormatter
                    .format(action.launchTime, at)}* after *${numRetries.show}* retries"),
                  Section(s"""|*ActionID:* ${action.uuid.show}""".stripMargin),
                  Section(s"""|*Service:* ${params.serviceParams.uniqueName}
                              |*Host:* ${params.serviceParams.taskParams.hostName}""".stripMargin)
                ) ::: (if (notes.value.isEmpty) Nil else List(Section(abbreviate(notes.value))))
              )
            )
          )
        }
        msg.flatMap(m => sns.publish(m.asJson.noSpaces)).whenA(params.importance === Importance.Critical)

      // no op
      case _: PassThrough => F.unit
    }
}
