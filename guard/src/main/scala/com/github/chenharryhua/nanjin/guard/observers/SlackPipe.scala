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
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.text.NumberFormat
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
        extraSlackSections = Async[F].pure(Nil),
        isLoggging = false
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
  extraSlackSections: F[List[Section]],
  isLoggging: Boolean
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

  def withLogging: SlackPipe[F] = updateSlackConfig(_.copy(isLoggging = true))

  private def toText(counters: Map[String, Long]): String = {
    val fmt: NumberFormat = NumberFormat.getIntegerInstance
    counters.map(x => s"${x._1}: *${fmt.format(x._2)}*").toList.sorted.mkString("\n")
  }

  private def hostServiceSection(sp: ServiceParams): Section = Section(
    s"*Host:* ${sp.taskParams.hostName} \t *Service:* ${sp.uniqueName}")

  private def took(from: ZonedDateTime, to: ZonedDateTime): String =
    cfg.durationFormatter.format(from, to)

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

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
                blocks =
                  services.toList.map(ss => Section(s"""|:octagonal_sign: Terminated Service *${ss._1.uniqueName}*
                                                        |*App:* ${ss._1.taskParams.appName}
                                                        |*Host:* ${ss._1.taskParams.hostName}
                                                        |*Up Time:* ${cfg.durationFormatter.format(ss._2.toInstant, ts)}""".stripMargin)) ::: extra
              ))
          ).asJson.spaces2
          _ <- sns.publish(msg).attempt.void
          _ <- logger.info(msg).whenA(cfg.isLoggging)
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
                  Section(s"""|:rocket: (Re)Started Service in time zone *${params.taskParams.zoneId.show}*
                              |*Up Time:* ${took(si.launchTime, at)}""".stripMargin),
                  hostServiceSection(params)
                ) ::: extra
              ))
          ))

        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

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
                              |The service experienced a panic, the *${toOrdinalWords(
                    details.retriesSoFar + 1L)}* time, $upcoming""".stripMargin),
                  Section(s"""|*Cumulative Delay:* ${cfg.durationFormatter.format(details.cumulativeDelay)} 
                              |*Retry Policy:* ${params.retry.policy[F].show}
                              |*Up Time:* ${took(si.launchTime, at)}""".stripMargin),
                  hostServiceSection(params)
                ) ::: extra ::: List(Section(s"*Cause*:\n```${abbreviate(error.stackTrace)}```"))
              )
            )
          ))
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ServiceAlert(_, _, params, importance, alertName, message) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = importance match {
                  case Importance.Critical => cfg.errorColor
                  case Importance.High     => cfg.warnColor
                  case Importance.Medium   => cfg.infoColor
                  case Importance.Low      => cfg.infoColor
                },
                blocks = List(
                  Section(alertName),
                  hostServiceSection(params),
                  Section(abbreviate(message))
                )
              )
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(importance =!= Importance.Low)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ServiceStopped(at, si, params, snapshot) =>
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.warnColor,
                blocks = List(
                  Section(s":octagonal_sign: Service Stopped after *${took(si.launchTime, at)}*."),
                  hostServiceSection(params)
                ) ::: extra
              ),
              Attachments(color = cfg.warnColor, blocks = List(Section(abbreviate(toText(snapshot.counters)))))
            )
          ))
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case MetricsReport(index, at, si, params, snapshot) =>
        val msg = cfg.extraSlackSections.map { extra =>
          val next = nextTime(params.metric.reportSchedule, at, cfg.reportInterval, si.launchTime)
            .fold("no report thereafter")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)

          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = if (snapshot.counters.keys.exists(_.contains('`'))) cfg.warnColor else cfg.infoColor,
                blocks = List(
                  Section(
                    s":gottarun: The service has been running for *${took(si.launchTime, at)}* in time zone *${params.taskParams.zoneId}*, next show at *$next*"),
                  hostServiceSection(params)
                ) ::: extra ::: List(Section(abbreviate(toText(snapshot.counters))))
              )
            )
          )
        }
        val isShow = isShowMetrics(params.metric.reportSchedule, at, cfg.reportInterval, si.launchTime) || index === 1L
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(isShow)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case MetricsReset(at, si, params, prev, next, snapshot) =>
        val toNow =
          prev.map(p => cfg.durationFormatter.format(Order.max(p, si.launchTime), at)).fold("")(dur => s" in past $dur")
        val summaries = s"*This is a summary of activities performed by the service$toNow*"
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = params.taskParams.appName,
            attachments = List(
              Attachments(
                color = if (snapshot.counters.keys.exists(_.contains('`'))) cfg.warnColor else cfg.infoColor,
                blocks = List(
                  Section(summaries),
                  Section(
                    s"""|*Next Reset:* ${next.fold("no time")(_.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show)}
                        |*Up Time:* ${took(si.launchTime, at)}""".stripMargin),
                  hostServiceSection(params)
                ) ::: extra ::: List(Section(abbreviate(toText(snapshot.counters))))
              )
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionStart(params, action) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.infoColor,
                blocks = List(
                  Section(s"""|Kick off action: *${params.uniqueName}*""".stripMargin),
                  Section(s"""|*ActionID:* ${action.uuid.show}""".stripMargin),
                  hostServiceSection(params.serviceParams)
                )
              ))
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(params.importance === Importance.Critical)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionRetrying(params, action, at, wdr, error) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.warnColor,
                blocks = List(
                  Section(
                    s"This is the *${toOrdinalWords(wdr.retriesSoFar + 1L)}* failure of the action *${params.uniqueName}*, so far took *${took(
                      action.launchTime,
                      at)}*, retry of which takes place in *${cfg.durationFormatter.format(wdr.nextDelay)}*"),
                  Section(s"""|*ActionID:* ${action.uuid.show}
                              |*Retry Policy:* ${params.retry.policy[F].show}""".stripMargin),
                  hostServiceSection(params.serviceParams),
                  Section(s"*Cause:* \n```${abbreviate(error.stackTrace)}```")
                )
              ))
          )
        }

        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(params.importance =!= Importance.Low && cfg.isShowRetry)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionFailed(params, action, at, numRetries, notes, error) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.errorColor,
                blocks = List(
                  Section(
                    s"The action *${params.uniqueName}* was failed after *${numRetries.show}* retries, took *${took(action.launchTime, at)}*"),
                  Section(s"""|*ActionID:* ${action.uuid.show}
                              |*ErrorID:* ${error.uuid.show}
                              |*Retry Policy:* ${params.retry.policy[F].show}
                              |""".stripMargin),
                  hostServiceSection(params.serviceParams),
                  Section(s"*Cause:* \n```${abbreviate(error.stackTrace)}```")
                ) ::: (if (notes.value.isEmpty) Nil else List(Section(abbreviate(notes.value))))
              )
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(params.importance =!= Importance.Low)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionSucced(params, action, at, numRetries, notes) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = params.serviceParams.taskParams.appName,
            attachments = List(
              Attachments(
                color = cfg.goodColor,
                blocks = List(
                  Section(
                    s"The action *${params.uniqueName}* was successfully completed in *${took(action.launchTime, at)}*, after *${numRetries.show}* retries"),
                  Section(s"""|*ActionID:* ${action.uuid.show}""".stripMargin),
                  hostServiceSection(params.serviceParams)
                ) ::: (if (notes.value.isEmpty) Nil else List(Section(abbreviate(notes.value))))
              )
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(params.importance === Importance.Critical)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case msg: PassThrough => logger.info(msg.asJson.spaces2).whenA(cfg.isLoggging)
    }
}
