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

final private[guard] class DefaultSlackTranslator[F[_]: Applicative](cfg: SlackConfig[F]) extends all {

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

  private def serviceStarted(ss: ServiceStart): F[SlackApp] =
    cfg.extraSlackSections.map(extra =>
      SlackApp(
        username = ss.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.infoColor,
            blocks = List(
              MarkdownSection(":rocket: *(Re)Started Service*"),
              hostServiceSection(ss.serviceParams),
              JuxtaposeSection(
                first = TextField("Up Time", cfg.durationFormatter.format(ss.upTime)),
                second = TextField("Time Zone", ss.serviceParams.taskParams.zoneId.show)
              )
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      ))

  private def servicePanic(sp: ServicePanic): F[SlackApp] = {
    val upcoming: String = sp.retryDetails.upcomingDelay.map(cfg.durationFormatter.format) match {
      case None => "report to developer once you see this message" // never happen
      case Some(ts) =>
        s"restart of which takes place in *$ts* meanwhile the service is dysfunctional. ${cfg.atSupporters}"
    }
    cfg.extraSlackSections.map(extra =>
      SlackApp(
        username = sp.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.errorColor,
            blocks = List(
              MarkdownSection(
                s":alarm: The service experienced a panic, the *${toOrdinalWords(sp.retryDetails.retriesSoFar + 1L)}* time, $upcoming"),
              hostServiceSection(sp.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", cfg.durationFormatter.format(sp.upTime)),
                TextField("Cummulative Delay", cfg.durationFormatter.format(sp.retryDetails.cumulativeDelay))
              ),
              MarkdownSection(s"*Restart Policy:* ${sp.serviceParams.retry.policy[F].show}"),
              MarkdownSection(s"*Error ID:* ${sp.error.uuid.show}"),
              KeyValueSection("Cause", s"```${abbreviate(sp.error.stackTrace)}```")
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      ))
  }

  private def serviceAlert(sa: ServiceAlert): SlackApp = {
    val (users, title, color) = sa.importance match {
      case Importance.Critical => (cfg.atSupporters, ":warning: Error", cfg.errorColor)
      case Importance.High     => ("", ":warning: Warning", cfg.warnColor)
      case Importance.Medium   => ("", ":information_source: Info", cfg.infoColor)
      case Importance.Low      => (cfg.atSupporters, "oops. should not happen", cfg.errorColor)
    }
    SlackApp(
      username = sa.serviceParams.taskParams.appName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(MarkdownSection(s"*$title:* ${sa.name.value} $users"), hostServiceSection(sa.serviceParams)) :::
            (if (sa.message.isEmpty) Nil else List(MarkdownSection(abbreviate(sa.message))))
        )
      )
    )
  }

  private def serviceStopped(ss: ServiceStop): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      SlackApp(
        username = ss.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.warnColor,
            blocks = List(
              MarkdownSection(s":octagonal_sign: *Service Stopped*. ${cfg.atSupporters}"),
              hostServiceSection(ss.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", cfg.durationFormatter.format(ss.upTime)),
                TextField("Time Zone", ss.serviceParams.taskParams.zoneId.show)),
              metricsSection(ss.snapshot)
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      )
    }

  private def metricsReport(mr: MetricsReport): F[Option[SlackApp]] = {
    val msg = cfg.extraSlackSections.map { extra =>
      val next =
        nextTime(mr.serviceParams.metric.reportSchedule, mr.timestamp, cfg.reportInterval, mr.serviceStatus.launchTime)
          .fold("None")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)

      SlackApp(
        username = mr.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = if (mr.snapshot.isContainErrors) cfg.warnColor else cfg.infoColor,
            blocks = List(
              MarkdownSection(s"${cfg.metricsReportEmoji} *${mr.reportType.show}*"),
              hostServiceSection(mr.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", cfg.durationFormatter.format(mr.upTime)),
                TextField("Scheduled Next", next)),
              metricsSection(mr.snapshot)
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      )
    }
    val isShow =
      isShowMetrics(
        mr.serviceParams.metric.reportSchedule,
        mr.timestamp,
        cfg.reportInterval,
        mr.serviceStatus.launchTime) || mr.reportType.isShow

    msg.map(m => if (isShow) Some(m) else None)
  }

  private def metricsReset(mr: MetricsReset): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      mr.resetType match {
        case MetricResetType.Adhoc =>
          SlackApp(
            username = mr.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection("*Adhoc Metric Reset*"),
                  hostServiceSection(mr.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", cfg.durationFormatter.format(mr.upTime)),
                    TextField(
                      "Scheduled Next",
                      mr.serviceParams.metric.resetSchedule
                        .flatMap(_.next(mr.timestamp.toInstant))
                        .map(
                          _.atZone(mr.serviceParams.taskParams.zoneId).toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)
                        .getOrElse("None")
                    )
                  ),
                  metricsSection(mr.snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
        case MetricResetType.Scheduled(next) =>
          SlackApp(
            username = mr.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection(s"*Scheduled Metric Reset*"),
                  hostServiceSection(mr.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", cfg.durationFormatter.format(mr.upTime)),
                    TextField("Scheduled Next", next.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show)
                  ),
                  metricsSection(mr.snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
      }
    }

  private def actionStart(as: ActionStart): Option[SlackApp] =
    if (as.actionParams.importance === Importance.Critical)
      Some(
        SlackApp(
          username = as.serviceParams.taskParams.appName,
          attachments = List(Attachment(
            color = cfg.infoColor,
            blocks = List(
              MarkdownSection(s"${cfg.startActionEmoji} *${as.actionParams.startTitle}*"),
              hostServiceSection(as.actionInfo.serviceParams),
              MarkdownSection(s"""|*${as.actionParams.alias} ID:* ${as.uuid.show}""".stripMargin)
            )
          ))
        ))
    else None

  private def actionRetrying(ar: ActionRetry): Option[SlackApp] =
    if (ar.actionParams.importance >= Importance.Medium) {
      Some(
        SlackApp(
          username = ar.serviceParams.taskParams.appName,
          attachments = List(Attachment(
            color = cfg.warnColor,
            blocks = List(
              MarkdownSection(s"${cfg.retryActionEmoji} *${ar.actionParams.retryTitle}*"),
              JuxtaposeSection(
                TextField("Took so far", cfg.durationFormatter.format(ar.took)),
                TextField("Retries so far", ar.willDelayAndRetry.retriesSoFar.show)),
              MarkdownSection(s"""|*${ar.actionParams.alias} ID:* ${ar.uuid.show}
                                  |*next retry in: * ${cfg.durationFormatter.format(ar.willDelayAndRetry.nextDelay)}
                                  |*policy:* ${ar.actionParams.retry.policy[F].show}""".stripMargin),
              hostServiceSection(ar.serviceParams),
              KeyValueSection("Cause", s"```${abbreviate(ar.error.stackTrace)}```")
            )
          ))
        ))
    } else None

  private def actionFailed(af: ActionFail): F[Option[SlackApp]] =
    cfg.extraSlackSections.map { extra =>
      if (af.actionParams.importance >= Importance.Medium) {
        Some(
          SlackApp(
            username = af.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.errorColor,
                blocks = List(
                  MarkdownSection(s"${cfg.failActionEmoji} *${af.actionParams.failedTitle}*"),
                  JuxtaposeSection(
                    TextField("Took", cfg.durationFormatter.format(af.took)),
                    TextField("Retries", af.numRetries.show)),
                  MarkdownSection(s"""|*${af.actionParams.alias} ID:* ${af.uuid.show}
                                      |*error ID:* ${af.error.uuid.show}
                                      |*policy:* ${af.actionParams.retry.policy[F].show}""".stripMargin),
                  hostServiceSection(af.serviceParams),
                  KeyValueSection("Cause", s"```${abbreviate(af.error.stackTrace)}```")
                ) ::: (if (af.notes.value.isEmpty) Nil
                       else List(MarkdownSection(abbreviate(af.notes.value))))
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          ))
      } else None
    }

  private def actionSucced(as: ActionSucc): Option[SlackApp] =
    if (as.actionParams.importance === Importance.Critical) {
      Some(
        SlackApp(
          username = as.serviceParams.taskParams.appName,
          attachments = List(
            Attachment(
              color = cfg.goodColor,
              blocks = List(
                MarkdownSection(s"${cfg.succActionEmoji} *${as.actionParams.succedTitle}*"),
                JuxtaposeSection(
                  TextField("Took", cfg.durationFormatter.format(as.took)),
                  TextField("Retries", as.numRetries.show)),
                MarkdownSection(s"*${as.actionParams.alias} ID:* ${as.uuid.show}"),
                hostServiceSection(as.serviceParams)
              ) ::: (if (as.notes.value.isEmpty) Nil
                     else List(MarkdownSection(abbreviate(as.notes.value))))
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
