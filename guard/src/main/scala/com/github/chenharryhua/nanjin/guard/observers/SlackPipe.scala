package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Async, Resource}
import cats.kernel.Order
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.{Importance, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.lib.javatime.javaTemporalInstance
import fs2.{Pipe, Stream}
import io.circe.Encoder
import io.circe.generic.auto.*
import io.circe.literal.JsonStringContext
import io.circe.syntax.*
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
        isLoggging = false,
        supporters = Nil,
        isShowMetrics = false
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
  isLoggging: Boolean,
  supporters: List[String],
  isShowMetrics: Boolean
) {
  val atSupporters: String =
    supporters
      .filter(_.nonEmpty)
      .map(_.trim)
      .map(spt => if (spt.startsWith("@") || spt.startsWith("<")) spt else s"@$spt")
      .distinct
      .mkString(" ")
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */

final private case class TextField(tag: String, value: String)
private object TextField {
  implicit val encodeTextField: Encoder[TextField] = tf => {
    val str = s"*${tf.tag}*\n${tf.value}"
    json"""
        {
           "type": "mrkdwn",
           "text": $str
        }
        """
  }
}
// slack format
sealed private trait Section
private object Section {
  implicit val encodeSection: Encoder[Section] = Encoder.instance {
    case JuxtaposeSection(first, second) =>
      json"""
            {
               "type": "section",
               "fields": ${List(first, second)}
            }
            """
    case MarkdownSection(text) =>
      json"""
            {
               "type": "section",
               "text": {
                          "type": "mrkdwn",
                          "text": $text
                       }
            }
            """
    case KeyValueSection(key, value) =>
      json"""
            {
               "type": "section",
               "text": ${TextField(key, value)}
            }
            """
  }
}

final private case class JuxtaposeSection(first: TextField, second: TextField) extends Section
final private case class KeyValueSection(tag: String, value: String) extends Section
final private case class MarkdownSection(text: String) extends Section

final private case class Attachment(color: String, blocks: List[Section])
final private case class SlackApp(username: String, attachments: List[Attachment])

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
    } yield esf :+ MarkdownSection(abbreviate(v))))

  def withSection(value: String): SlackPipe[F] = withSection(F.pure(value))

  /** show one message in every interval
    */
  def withReportInterval(interval: FiniteDuration): SlackPipe[F] =
    updateSlackConfig(_.copy(reportInterval = Some(interval)))

  def withoutReportInterval: SlackPipe[F] =
    updateSlackConfig(_.copy(reportInterval = None))

  def withLogging: SlackPipe[F] = updateSlackConfig(_.copy(isLoggging = true))

  def showMetricsWhenApplicable: SlackPipe[F] = updateSlackConfig(_.copy(isShowMetrics = true))

  def at(supporter: String): SlackPipe[F] = updateSlackConfig(c => c.copy(supporters = supporter :: c.supporters))
  def at(supporters: List[String]): SlackPipe[F] =
    updateSlackConfig(c => c.copy(supporters = supporters ::: c.supporters))

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  private val MessageSizeLimits: Int          = 2960
  private def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MessageSizeLimits)

  private def metricsSection(snapshot: MetricsSnapshot): KeyValueSection =
    if (cfg.isShowMetrics && snapshot.show.length <= MessageSizeLimits) {
      KeyValueSection("Metrics", s"```${snapshot.show}```")
    } else {
      val fmt: NumberFormat = NumberFormat.getIntegerInstance
      val msg: List[String] = snapshot.counters.map(x => s"${x._1}: ${fmt.format(x._2)}").toList.sorted
      KeyValueSection("Counters", s"```${abbreviate(msg.mkString("\n"))}```")
    }

  private def hostServiceSection(sp: ServiceParams): JuxtaposeSection =
    JuxtaposeSection(TextField("Service", MetricName(sp).value), TextField("Host", sp.taskParams.hostName))

  private def took(from: ZonedDateTime, to: ZonedDateTime): String =
    cfg.durationFormatter.format(from, to)

  private val logger: SelfAwareStructuredLogger[F] = Slf4jLogger.getLogger[F]

  override def apply(es: Stream[F, NJEvent]): Stream[F, NJEvent] =
    for {
      sns <- Stream.resource(snsResource)
      ref <- Stream.eval(F.ref[Set[ServiceInfo]](Set.empty))
      event <- es.evalMap { e =>
        val updateRef: F[Unit] = e match {
          case ServiceStarted(info, _)    => ref.update(_.incl(info))
          case ServiceStopped(info, _, _) => ref.update(_.excl(info))
          case _                          => F.unit
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
              Attachment(
                color = cfg.warnColor,
                blocks = List(MarkdownSection(s":octagonal_sign: *Terminated Service(s)* ${cfg.atSupporters}")))) :::
              services.toList.map(ss =>
                Attachment(
                  color = cfg.warnColor,
                  blocks = List(
                    hostServiceSection(ss.serviceParams),
                    JuxtaposeSection(
                      TextField("Up Time", cfg.durationFormatter.format(ss.launchTime.toInstant, ts)),
                      TextField("App", ss.serviceParams.taskParams.appName))
                  )
                )) ::: List(Attachment(color = cfg.infoColor, blocks = extra))
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

  private def publish(event: NJEvent, sns: SimpleNotificationService[F]): F[Unit] =
    event match {
      case ServiceStarted(si, at) =>
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = si.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection(":rocket: *(Re)Started Service*"),
                  hostServiceSection(si.serviceParams),
                  JuxtaposeSection(
                    first = TextField("Up Time", took(si.launchTime, at)),
                    second = TextField("Time Zone", si.serviceParams.taskParams.zoneId.show))
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          ))

        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ServicePanic(si, at, details, error) =>
        val upcoming: String = details.upcomingDelay.map(cfg.durationFormatter.format) match {
          case None => "report to developer once you see this message" // never happen
          case Some(ts) =>
            s"restart of which takes place in *$ts* meanwhile the service is dysfunctional. ${cfg.atSupporters}"
        }
        val msg = cfg.extraSlackSections.map(extra =>
          SlackApp(
            username = si.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.errorColor,
                blocks = List(
                  MarkdownSection(
                    s":alarm: The service experienced a panic, the *${toOrdinalWords(details.retriesSoFar + 1L)}* time, $upcoming"),
                  hostServiceSection(si.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", took(si.launchTime, at)),
                    TextField("Cummulative Delay", cfg.durationFormatter.format(details.cumulativeDelay))),
                  MarkdownSection(s"*Retry Policy:* ${si.serviceParams.retry.policy[F].show}"),
                  KeyValueSection("Cause", s"```${abbreviate(error.stackTrace)}```")
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          ))
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ServiceAlert(metricName, si, _, importance, message) =>
        val msg = cfg.extraSlackSections.map { extra =>
          val (users, title, color) = importance match {
            case Importance.Critical => (cfg.atSupporters, ":warning: Error", cfg.errorColor)
            case Importance.High     => ("", ":warning: Warning", cfg.warnColor)
            case Importance.Medium   => ("", ":information_source: Info", cfg.infoColor)
            case Importance.Low      => ("", "Not/Applicable/Yet", cfg.infoColor)
          }
          SlackApp(
            username = si.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = color,
                blocks = List(
                  MarkdownSection(s"*$title:* ${metricName.value} $users"),
                  hostServiceSection(si.serviceParams)) :::
                  (if (message.isEmpty) Nil else List(MarkdownSection(abbreviate(message))))
              )
            ) :+ Attachment(color = cfg.infoColor, blocks = extra)
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(importance =!= Importance.Low)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ServiceStopped(si, at, snapshot) =>
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = si.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.warnColor,
                blocks = List(
                  MarkdownSection(s":octagonal_sign: *Service Stopped*."),
                  hostServiceSection(si.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", took(si.launchTime, at)),
                    TextField("Time Zone", si.serviceParams.taskParams.zoneId.show)),
                  metricsSection(snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case MetricsReport(rt, si, at, snapshot) =>
        val msg = cfg.extraSlackSections.map { extra =>
          val next = nextTime(si.serviceParams.metric.reportSchedule, at, cfg.reportInterval, si.launchTime)
            .fold("None")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)

          val name = rt match {
            case MetricReportType.AdventiveReport    => "Adventive Health Check"
            case MetricReportType.ScheduledReport(_) => "Health Check"
          }
          val color =
            if (snapshot.counters.keys.exists(_.startsWith(EventPublisher.ATTENTION))) cfg.warnColor else cfg.infoColor

          SlackApp(
            username = si.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = color,
                blocks = List(
                  MarkdownSection(s":health_worker: *$name*"),
                  hostServiceSection(si.serviceParams),
                  JuxtaposeSection(TextField("Up Time", took(si.launchTime, at)), TextField("Scheduled Next", next)),
                  metricsSection(snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
        }
        val isShow =
          isShowMetrics(si.serviceParams.metric.reportSchedule, at, cfg.reportInterval, si.launchTime) || rt.isShow
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(isShow)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case MetricsReset(rt, si, at, snapshot) =>
        val msg = cfg.extraSlackSections.map { extra =>
          val color =
            if (snapshot.counters.keys.exists(_.startsWith(EventPublisher.ATTENTION))) cfg.warnColor else cfg.infoColor
          rt match {
            case MetricResetType.AdventiveReset =>
              SlackApp(
                username = si.serviceParams.taskParams.appName,
                attachments = List(
                  Attachment(
                    color = color,
                    blocks = List(
                      MarkdownSection("*Adventive Metrics Reset*"),
                      hostServiceSection(si.serviceParams),
                      JuxtaposeSection(
                        TextField("Up Time", took(si.launchTime, at)),
                        TextField(
                          "Scheduled Next",
                          si.serviceParams.metric.resetSchedule
                            .flatMap(_.next(at.toInstant))
                            .map(_.atZone(si.serviceParams.taskParams.zoneId).toLocalTime
                              .truncatedTo(ChronoUnit.SECONDS)
                              .show)
                            .getOrElse("None")
                        )
                      ),
                      metricsSection(snapshot)
                    )
                  ),
                  Attachment(color = cfg.infoColor, blocks = extra)
                )
              )
            case MetricResetType.ScheduledReset(prev, next) =>
              val dur = cfg.durationFormatter.format(Order.max(prev, si.launchTime), at)
              SlackApp(
                username = si.serviceParams.taskParams.appName,
                attachments = List(
                  Attachment(
                    color = color,
                    blocks = List(
                      MarkdownSection(
                        s"*This is schedued summary of activities performed by the service in past $dur*"),
                      hostServiceSection(si.serviceParams),
                      JuxtaposeSection(
                        TextField("Up Time", took(si.launchTime, at)),
                        TextField("Scheduled Next", next.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show)),
                      metricsSection(snapshot)
                    )
                  ),
                  Attachment(color = cfg.infoColor, blocks = extra)
                )
              )
          }
        }

        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionStart(action) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = action.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection(s"""|Kick off action: *${action.actionParams.metricName.value}*""".stripMargin),
                  MarkdownSection(s"""|*Action ID:* ${action.uuid.show}""".stripMargin),
                  hostServiceSection(action.serviceInfo.serviceParams)
                )
              ))
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(action.actionParams.importance === Importance.Critical)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionRetrying(action, at, wdr, error) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = action.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.warnColor,
                blocks = List(
                  MarkdownSection(
                    s"This is the *${toOrdinalWords(wdr.retriesSoFar + 1L)}* failure of the action *${action.actionParams.metricName.value}*, so far took *${took(
                      action.launchTime,
                      at)}*, retry of which takes place in *${cfg.durationFormatter.format(wdr.nextDelay)}*"),
                  MarkdownSection(s"""|*Action ID:* ${action.uuid.show}
                                      |*Retry Policy:* ${action.actionParams.retry.policy[F].show}""".stripMargin),
                  hostServiceSection(action.serviceInfo.serviceParams),
                  KeyValueSection("Cause", s"```${abbreviate(error.stackTrace)}```")
                )
              ))
          )
        }

        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(action.actionParams.importance =!= Importance.Low && cfg.isShowRetry)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionFailed(action, at, numRetries, notes, error) =>
        val msg = cfg.extraSlackSections.map { extra =>
          SlackApp(
            username = action.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.errorColor,
                blocks = List(
                  MarkdownSection(
                    s"The action *${action.actionParams.metricName.value}* was failed after *${numRetries.show}* retries, took *${took(
                      action.launchTime,
                      at)}* ${cfg.atSupporters}"),
                  MarkdownSection(s"""|*Action ID:* ${action.uuid.show}
                                      |*Error ID:* ${error.uuid.show}
                                      |*Retry Policy:* ${action.actionParams.retry.policy[F].show}
                                      |""".stripMargin),
                  hostServiceSection(action.serviceInfo.serviceParams),
                  KeyValueSection("Cause", s"```${abbreviate(error.stackTrace)}```")
                ) ::: (if (notes.value.isEmpty) Nil else List(MarkdownSection(abbreviate(notes.value))))
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(action.actionParams.importance =!= Importance.Low)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      case ActionSucced(action, at, numRetries, notes) =>
        val msg = cfg.extraSlackSections.map { _ =>
          SlackApp(
            username = action.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.goodColor,
                blocks = List(
                  MarkdownSection(
                    s"The action *${action.actionParams.metricName.value}* was accomplished in *${took(action.launchTime, at)}*, after *${numRetries.show}* retries"),
                  MarkdownSection(s"*Action ID:* ${action.uuid.show}"),
                  hostServiceSection(action.serviceInfo.serviceParams)
                ) ::: (if (notes.value.isEmpty) Nil else List(MarkdownSection(abbreviate(notes.value))))
              )
            )
          )
        }
        for {
          m <- msg.map(_.asJson.spaces2)
          _ <- sns.publish(m).whenA(action.actionParams.importance === Importance.Critical)
          _ <- logger.info(m).whenA(cfg.isLoggging)
        } yield ()

      // not sent to slack
      case _: PassThrough => F.unit
    }
}
