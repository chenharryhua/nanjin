package com.github.chenharryhua.nanjin.guard.observers

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.aws.SnsArn
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.{INothing, Pipe, Stream}
import io.chrisdavenport.cats.time.instances.zoneid
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConverters.*
object slack {
  def apply[F[_]: Sync](snsResource: Resource[F, SimpleNotificationService[F]]): Pipe[F, NJEvent, INothing] =
    new SlackSink[F](snsResource).sink

  def apply[F[_]: Sync](snsArn: SnsArn): Pipe[F, NJEvent, INothing] = apply[F](SimpleNotificationService[F](snsArn))
}

/** Notes: slack messages [[https://api.slack.com/docs/messages/builder]]
  */
final private case class SlackField(title: String, value: String, short: Boolean)

final private case class Attachment(color: String, ts: Long, fields: List[SlackField])
final private case class SlackNotification(username: String, text: String, attachments: List[Attachment])

final private class SlackSink[F[_]](snsResource: Resource[F, SimpleNotificationService[F]])(implicit F: Sync[F])
    extends zoneid {

  private def toOrdinalWords(n: Int): String = n + {
    if (n % 100 / 10 == 1) "th"
    else
      n % 10 match {
        case 1 => "st"
        case 2 => "nd"
        case 3 => "rd"
        case _ => "th"
      }
  }

  private val good_color  = "good"
  private val warn_color  = "#ffd79a"
  private val info_color  = "#b3d1ff"
  private val error_color = "danger"

  private val maxCauseSize: Int = 500

  val sink: Pipe[F, NJEvent, INothing] = (es: Stream[F, NJEvent]) =>
    Stream.resource(snsResource).flatMap(s => es.evalMap(e => send(e, s))).drain

  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  private def translate(mrw: MetricRegistryWrapper): String =
    mrw.value.fold("") { mr =>
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
        sns.publish(msg).void

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
                  SlackField("Cause", StringUtils.abbreviate(error.message, maxCauseSize), short = false)
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

        sns.publish(msg).void

      case MetricsReport(_, at, si, params, next, metrics) =>
        def msg: String = SlackNotification(
          params.taskParams.appName,
          s""":gottarun: Heath Check is scheduled by ${params.reportingSchedule.fold(
            d => s"fixed rate: ${fmt.format(d)}",
            c => s"crontab: ${c.toString}")} 
             |${StringUtils.abbreviate(translate(metrics), maxCauseSize)}""".stripMargin,
          List(
            Attachment(
              info_color,
              at.toInstant.toEpochMilli,
              List(
                SlackField("Service", params.serviceName, short = true),
                SlackField("Host", params.taskParams.hostName, short = true),
                SlackField("Up Time", fmt.format(si.launchTime, at), short = true),
                SlackField("Next Check in", next.fold("No more checking thereafter")(fmt.format), short = true),
                SlackField("Brief", params.brief, short = false)
              )
            ))
        ).asJson.noSpaces

        sns.publish(msg).void

      case ActionStart(params, action, at) =>
        def msg: String =
          SlackNotification(
            params.serviceParams.taskParams.appName,
            s"Start running action: *${params.actionName}*",
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
        sns.publish(msg).void

      case ActionRetrying(params, action, at, wdr, error) =>
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
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Action ID", action.id.show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, maxCauseSize), short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).void

      case af @ ActionFailed(params, action, at, numRetries, notes, error) =>
        def msg: String =
          SlackNotification(
            params.serviceParams.taskParams.appName,
            notes.value,
            List(
              Attachment(
                if (af.importance.value > Importance.Medium.value) error_color else warn_color,
                at.toInstant.toEpochMilli,
                List(
                  SlackField("Service", params.serviceParams.serviceName, short = true),
                  SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Importance", af.importance.show, short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Retried", numRetries.show, short = true),
                  SlackField("Retry Policy", params.retry.policy[F].show, short = false),
                  SlackField("Action ID", action.id.show, short = false),
                  SlackField("Cause", StringUtils.abbreviate(error.message, maxCauseSize), short = false)
                )
              ))
          ).asJson.noSpaces
        sns.publish(msg).void

      case ActionSucced(params, action, at, numRetries, notes) =>
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
                  SlackField("Action", params.actionName, short = true),
                  SlackField("Status", "Completed", short = true),
                  SlackField("Took", fmt.format(action.launchTime, at), short = true),
                  SlackField("Retried", s"$numRetries/${params.retry.maxRetries}", short = true),
                  SlackField("Action ID", action.id.show, short = false)
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
                  good_color,
                  at.toInstant.toEpochMilli,
                  List(
                    SlackField("Service", params.serviceParams.serviceName, short = true),
                    SlackField("Host", params.serviceParams.taskParams.hostName, short = true),
                    SlackField("Action", params.actionName, short = true),
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
                    SlackField("Action", params.actionName, short = true),
                    SlackField("Status", "Quasi Success", short = true),
                    SlackField("Succed", numSucc.show, short = true),
                    SlackField("Failed", errors.size.show, short = true),
                    SlackField("Took", fmt.format(action.launchTime, at), short = true),
                    SlackField("Run Mode", runMode.show, short = true),
                    SlackField("Action ID", action.id.show, short = false)
                  )
                ))
            )

        sns.publish(msg.asJson.noSpaces).void

      case ForYourInformation(_, message) => sns.publish(message).void

      // no op
      case _: PassThrough => F.unit
    }
}
