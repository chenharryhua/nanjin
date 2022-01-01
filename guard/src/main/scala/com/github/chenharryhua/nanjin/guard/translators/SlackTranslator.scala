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

final class SlackTranslator[F[_]: Applicative](cfg: SlackConfig[F]) extends all {

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

  private def serviceStarted(evt: ServiceStart): F[SlackApp] =
    cfg.extraSlackSections.map(extra =>
      SlackApp(
        username = evt.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.infoColor,
            blocks = List(
              MarkdownSection(":rocket: *(Re)Started Service*"),
              hostServiceSection(evt.serviceParams),
              JuxtaposeSection(
                first = TextField("Up Time", cfg.durationFormatter.format(evt.upTime)),
                second = TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)
              )
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      ))

  private def servicePanic(evt: ServicePanic): F[SlackApp] = {
    val upcoming: String = evt.retryDetails.upcomingDelay.map(cfg.durationFormatter.format) match {
      case None => "report to developer once you see this message" // never happen
      case Some(ts) =>
        s"restart of which takes place in *$ts* meanwhile the service is dysfunctional. ${cfg.atSupporters}"
    }
    cfg.extraSlackSections.map(extra =>
      SlackApp(
        username = evt.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.errorColor,
            blocks = List(
              MarkdownSection(
                s":alarm: The service experienced a panic, the *${toOrdinalWords(evt.retryDetails.retriesSoFar + 1L)}* time, $upcoming"),
              hostServiceSection(evt.serviceParams),
              MarkdownSection(s"""|*Up Time:* ${cfg.durationFormatter.format(evt.upTime)}
                                  |*Restart Policy:* ${evt.serviceParams.retry.policy[F].show}
                                  |*Error ID:* ${evt.error.uuid.show}
                                  |*Cause:* ${evt.error.message}""".stripMargin)
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      ))
  }

  private def serviceAlert(evt: ServiceAlert): SlackApp = {
    val (users, title, color) = evt.importance match {
      case Importance.Critical => (cfg.atSupporters, ":warning: Error", cfg.errorColor)
      case Importance.High     => ("", ":warning: Warning", cfg.warnColor)
      case Importance.Medium   => ("", ":information_source: Info", cfg.infoColor)
      case Importance.Low      => (cfg.atSupporters, "oops. should not happen", cfg.errorColor)
    }
    SlackApp(
      username = evt.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = color,
          blocks =
            List(MarkdownSection(s"*$title:* ${evt.name.value} $users"), hostServiceSection(evt.serviceParams)) :::
              (if (evt.message.isEmpty) Nil else List(MarkdownSection(abbreviate(evt.message))))
        )
      )
    )
  }

  private def serviceStopped(evt: ServiceStop): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      SlackApp(
        username = evt.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.warnColor,
            blocks = List(
              MarkdownSection(s":octagonal_sign: *Service Stopped*. ${cfg.atSupporters}"),
              hostServiceSection(evt.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", cfg.durationFormatter.format(evt.upTime)),
                TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)),
              metricsSection(evt.snapshot)
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      )
    }

  private def metricsReport(evt: MetricsReport): F[Option[SlackApp]] = {
    val msg = cfg.extraSlackSections.map { extra =>
      val next =
        nextTime(
          evt.serviceParams.metric.reportSchedule,
          evt.timestamp,
          cfg.reportInterval,
          evt.serviceStatus.launchTime).fold("None")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)

      SlackApp(
        username = evt.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = if (evt.snapshot.isContainErrors) cfg.warnColor else cfg.infoColor,
            blocks = List(
              MarkdownSection(s"${cfg.metricsReportEmoji} *${evt.reportType.show}*"),
              hostServiceSection(evt.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", cfg.durationFormatter.format(evt.upTime)),
                TextField("Scheduled Next", next)),
              metricsSection(evt.snapshot)
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      )
    }
    val isShow =
      isShowMetrics(
        evt.serviceParams.metric.reportSchedule,
        evt.timestamp,
        cfg.reportInterval,
        evt.serviceStatus.launchTime) || evt.reportType.isShow

    msg.map(m => if (isShow) Some(m) else None)
  }

  private def metricsReset(evt: MetricsReset): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      evt.resetType match {
        case MetricResetType.Adhoc =>
          SlackApp(
            username = evt.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection("*Adhoc Metric Reset*"),
                  hostServiceSection(evt.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", cfg.durationFormatter.format(evt.upTime)),
                    TextField(
                      "Scheduled Next",
                      evt.serviceParams.metric.resetSchedule
                        .flatMap(_.next(evt.timestamp.toInstant))
                        .map(_.atZone(evt.serviceParams.taskParams.zoneId).toLocalTime
                          .truncatedTo(ChronoUnit.SECONDS)
                          .show)
                        .getOrElse("None")
                    )
                  ),
                  metricsSection(evt.snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
        case MetricResetType.Scheduled(next) =>
          SlackApp(
            username = evt.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection(s"*Scheduled Metric Reset*"),
                  hostServiceSection(evt.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", cfg.durationFormatter.format(evt.upTime)),
                    TextField("Scheduled Next", next.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show)
                  ),
                  metricsSection(evt.snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
      }
    }

  private def actionStart(evt: ActionStart): Option[SlackApp] =
    if (evt.actionParams.importance === Importance.Critical)
      Some(
        SlackApp(
          username = evt.serviceParams.taskParams.appName,
          attachments = List(Attachment(
            color = cfg.infoColor,
            blocks = List(
              MarkdownSection(s"${cfg.startActionEmoji} *${evt.actionParams.startTitle}*"),
              hostServiceSection(evt.actionInfo.serviceParams),
              MarkdownSection(s"*${evt.actionParams.alias} ID:* ${evt.uuid.show}")
            )
          ))
        ))
    else None

  private def actionRetrying(evt: ActionRetry): Option[SlackApp] =
    if (evt.actionParams.importance >= Importance.Medium) {
      Some(
        SlackApp(
          username = evt.serviceParams.taskParams.appName,
          attachments = List(Attachment(
            color = cfg.warnColor,
            blocks = List(
              MarkdownSection(s"${cfg.retryActionEmoji} *${evt.actionParams.retryTitle}*"),
              JuxtaposeSection(
                TextField("Took so far", cfg.durationFormatter.format(evt.took)),
                TextField("Retries so far", evt.willDelayAndRetry.retriesSoFar.show)),
              MarkdownSection(s"""|*${evt.actionParams.alias} ID:* ${evt.uuid.show}
                                  |*next retry in: * ${cfg.durationFormatter.format(evt.willDelayAndRetry.nextDelay)}
                                  |*policy:* ${evt.actionParams.retry.policy[F].show}""".stripMargin),
              hostServiceSection(evt.serviceParams),
              MarkdownSection(s"*Cause:* ${evt.error.message}")
            )
          ))
        ))
    } else None

  private def actionFailed(evt: ActionFail): F[Option[SlackApp]] =
    cfg.extraSlackSections.map { extra =>
      if (evt.actionParams.importance >= Importance.Medium) {
        Some(
          SlackApp(
            username = evt.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.errorColor,
                blocks = List(
                  MarkdownSection(s"${cfg.failActionEmoji} *${evt.actionParams.failedTitle}*"),
                  JuxtaposeSection(
                    TextField("Took", cfg.durationFormatter.format(evt.took)),
                    TextField("Retries", evt.numRetries.show)),
                  MarkdownSection(s"""|*${evt.actionParams.alias} ID:* ${evt.uuid.show}
                                      |*error ID:* ${evt.error.uuid.show}
                                      |*policy:* ${evt.actionParams.retry.policy[F].show}""".stripMargin),
                  hostServiceSection(evt.serviceParams),
                  MarkdownSection(s"*Cause:* ${evt.error.message}")
                ) ::: (if (evt.notes.value.isEmpty) Nil
                       else List(MarkdownSection(abbreviate(evt.notes.value))))
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          ))
      } else None
    }

  private def actionSucced(evt: ActionSucc): Option[SlackApp] =
    if (evt.actionParams.importance === Importance.Critical) {
      Some(
        SlackApp(
          username = evt.serviceParams.taskParams.appName,
          attachments = List(
            Attachment(
              color = cfg.goodColor,
              blocks = List(
                MarkdownSection(s"${cfg.succActionEmoji} *${evt.actionParams.succedTitle}*"),
                JuxtaposeSection(
                  TextField("Took", cfg.durationFormatter.format(evt.took)),
                  TextField("Retries", evt.numRetries.show)),
                MarkdownSection(s"*${evt.actionParams.alias} ID:* ${evt.uuid.show}"),
                hostServiceSection(evt.serviceParams)
              ) ::: (if (evt.notes.value.isEmpty) Nil
                     else List(MarkdownSection(abbreviate(evt.notes.value))))
            )
          )
        ))
    } else None

  def translator: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic)
      .withServiceStop(serviceStopped)
      .withMetricsReport(metricsReport)
      .withMetricsReset(metricsReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionSucc(actionSucced)
}
