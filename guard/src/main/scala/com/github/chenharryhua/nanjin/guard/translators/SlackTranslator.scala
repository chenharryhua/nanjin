package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.lib.javatime.javaTemporalInstance
import io.circe.generic.auto.*
import org.typelevel.cats.time.instances.all

import java.text.NumberFormat
import java.time.temporal.ChronoUnit
import scala.jdk.DurationConverters.ScalaDurationOps

private[translators] object SlackTranslator extends all {
  private val goodColor  = "#36a64f"
  private val warnColor  = "#ffd79a"
  private val infoColor  = "#b3d1ff"
  private val errorColor = "#935252"

  private def metricsSection(snapshot: MetricSnapshot): KeyValueSection =
    if (snapshot.show.length <= MessageSizeLimits) {
      KeyValueSection("Metrics", s"```${snapshot.show.replace("-- ", "")}```")
    } else {
      val fmt: NumberFormat = NumberFormat.getIntegerInstance
      val msg: String =
        snapshot.counterMap.filter(_._2 > 0).map(x => s"${x._1}: ${fmt.format(x._2)}").toList.sorted.mkString("\n")
      if (msg.isEmpty)
        KeyValueSection("Counters", "*No counter update*")
      else
        KeyValueSection("Counters", s"```${abbreviate(msg)}```")
    }

  private def serviceStarted(evt: ServiceStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = infoColor,
          blocks = List(
            MarkdownSection(":rocket: *(Re)Started Service*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField("Up Time", fmt.format(evt.upTime)),
              second = TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)
            )
          )
        )
      )
    )

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): SlackApp = {
    val upcoming: String = evt.retryDetails.upcomingDelay match {
      case None => "the service was stopped" // never happen
      case Some(fd) =>
        s"restart of which takes place in *${fmt.format(fd)}*, at ${localTimestampStr(evt.timestamp.plus(fd.toJava))}," +
          " meanwhile the service is dysfunctional."
    }

    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = errorColor,
          blocks = List(
            MarkdownSection(
              s":alarm: The service experienced a panic, the *${toOrdinalWords(evt.retryDetails.retriesSoFar + 1L)}* time, $upcoming"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Restart Policy:* ${evt.serviceParams.retry.policy[F].show}
                                |*Error ID:* ${evt.error.uuid.show}
                                |*Cause:* ${evt.error.message}""".stripMargin)
          )
        )
      )
    )
  }

  private def serviceAlert(evt: ServiceAlert): SlackApp = {
    val (title, color) = evt.importance match {
      case Importance.Critical => (":warning: Error", errorColor)
      case Importance.High     => (":warning: Warning", warnColor)
      case Importance.Medium   => (":information_source: Info", infoColor)
      case Importance.Low      => ("oops. should not happen", errorColor)
    }
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(MarkdownSection(s"*$title:* ${evt.name.value}"), hostServiceSection(evt.serviceParams)) :::
            (if (evt.message.isEmpty) Nil else List(MarkdownSection(abbreviate(evt.message))))
        )
      )
    )
  }

  private def serviceStopped(evt: ServiceStop): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = warnColor,
          blocks = List(
            MarkdownSection(s":octagonal_sign: *Service Stopped*."),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              TextField("Up Time", fmt.format(evt.upTime)),
              TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)),
            metricsSection(evt.snapshot)
          )
        )
      )
    )

  private def metricsReport(evt: MetricsReport): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = if (evt.hasError) warnColor else infoColor,
          blocks = List(
            MarkdownSection(s"*${evt.reportType.show}*"),
            MarkdownSection(serviceStatusWord(evt.serviceStatus)),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              TextField("Up Time", fmt.format(evt.upTime)),
              TextField(
                "Scheduled Next",
                evt.serviceParams.metric.nextReport(evt.timestamp).map(localTimestampStr).getOrElse("None"))
            ),
            metricsSection(evt.snapshot)
          )
        )
      )
    )

  private def metricsReset(evt: MetricsReset): SlackApp =
    evt.resetType match {
      case MetricResetType.Adhoc =>
        SlackApp(
          username = evt.serviceParams.taskParams.appName,
          attachments = List(
            Attachment(
              color = if (evt.hasError) warnColor else infoColor,
              blocks = List(
                MarkdownSection("*Adhoc Metric Reset*"),
                MarkdownSection(serviceStatusWord(evt.serviceStatus)),
                hostServiceSection(evt.serviceParams),
                JuxtaposeSection(
                  TextField("Up Time", fmt.format(evt.upTime)),
                  TextField(
                    "Scheduled Next",
                    evt.serviceParams.metric.resetSchedule
                      .flatMap(_.next(evt.timestamp.toInstant))
                      .map(
                        _.atZone(evt.serviceParams.taskParams.zoneId).toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)
                      .getOrElse("None")
                  )
                ),
                metricsSection(evt.snapshot)
              )
            )
          )
        )
      case MetricResetType.Scheduled(next) =>
        SlackApp(
          username = evt.serviceParams.taskParams.appName,
          attachments = List(
            Attachment(
              color = if (evt.hasError) warnColor else infoColor,
              blocks = List(
                MarkdownSection(s"*Scheduled Metric Reset*"),
                MarkdownSection(serviceStatusWord(evt.serviceStatus)),
                hostServiceSection(evt.serviceParams),
                JuxtaposeSection(
                  TextField("Up Time", fmt.format(evt.upTime)),
                  TextField("Scheduled Next", next.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show)
                ),
                metricsSection(evt.snapshot)
              )
            )
          )
        )
    }

  private def actionStart(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = infoColor,
          blocks = List(
            MarkdownSection(s"*${evt.actionParams.startTitle}*"),
            hostServiceSection(evt.actionInfo.serviceParams),
            MarkdownSection(s"*${evt.actionParams.alias} ID:* ${evt.uuid.show}")
          )
        ))
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = warnColor,
          blocks = List(
            MarkdownSection(s"*${evt.actionParams.retryTitle}*"),
            JuxtaposeSection(
              TextField("Took so far", fmt.format(evt.took)),
              TextField("Retries so far", evt.willDelayAndRetry.retriesSoFar.show)),
            MarkdownSection(s"""|*${evt.actionParams.alias} ID:* ${evt.uuid.show}
                                |*next retry in: * ${fmt.format(evt.willDelayAndRetry.nextDelay)}
                                |*policy:* ${evt.actionParams.retry.policy[F].show}""".stripMargin),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*Cause:* ${evt.error.message}")
          )
        ))
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = errorColor,
          blocks = List(
            MarkdownSection(s"*${evt.actionParams.failedTitle}*"),
            JuxtaposeSection(TextField("Took", fmt.format(evt.took)), TextField("Retries", evt.numRetries.show)),
            MarkdownSection(s"""|*${evt.actionParams.alias} ID:* ${evt.uuid.show}
                                |*error ID:* ${evt.error.uuid.show}
                                |*policy:* ${evt.actionParams.retry.policy[F].show}""".stripMargin),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*Cause:* ${evt.error.message}")
          ) ::: (if (evt.notes.value.isEmpty) Nil
                 else List(MarkdownSection(abbreviate(evt.notes.value))))
        )
      )
    )

  private def actionSucced(evt: ActionSucc): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = goodColor,
          blocks = List(
            MarkdownSection(s"*${evt.actionParams.succedTitle}*"),
            JuxtaposeSection(TextField("Took", fmt.format(evt.took)), TextField("Retries", evt.numRetries.show)),
            MarkdownSection(s"*${evt.actionParams.alias} ID:* ${evt.uuid.show}"),
            hostServiceSection(evt.serviceParams)
          ) ::: (if (evt.notes.value.isEmpty) Nil
                 else List(MarkdownSection(abbreviate(evt.notes.value))))
        )
      )
    )

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic[F])
      .withServiceStop(serviceStopped)
      .withMetricsReport(metricsReport)
      .withMetricsReset(metricsReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying[F])
      .withActionFail(actionFailed[F])
      .withActionSucc(actionSucced)
}
