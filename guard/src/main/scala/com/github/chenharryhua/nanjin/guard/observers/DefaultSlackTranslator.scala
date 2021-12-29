package com.github.chenharryhua.nanjin.guard.observers
import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.lib.javatime.javaTemporalInstance
import io.circe.generic.auto.*
import org.typelevel.cats.time.instances.all

import java.text.NumberFormat
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

final private[observers] class DefaultSlackTranslator[F[_]: Applicative](cfg: SlackConfig[F]) extends all {

  private def metricsSection(snapshot: MetricsSnapshot): KeyValueSection =
    if (cfg.isShowMetrics && snapshot.show.length <= MessageSizeLimits) {
      KeyValueSection("Metrics", s"```${snapshot.show.replace("-- ", "")}```")
    } else {
      val fmt: NumberFormat = NumberFormat.getIntegerInstance
      val msg: String =
        snapshot.counters.filter(_._2 > 0).map(x => s"${x._1}: ${fmt.format(x._2)}").toList.sorted.mkString("\n")
      if (msg.isEmpty)
        KeyValueSection("Counters", "*No counter update*")
      else
        KeyValueSection("Counters", s"```${abbreviate(msg)}```")
    }

  private def took(from: ZonedDateTime, to: ZonedDateTime): String =
    cfg.durationFormatter.format(from, to)

  private def serviceStarted(ss: ServiceStarted): F[SlackApp] =
    cfg.extraSlackSections.map(extra =>
      SlackApp(
        username = ss.serviceInfo.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.infoColor,
            blocks = List(
              MarkdownSection(":rocket: *(Re)Started Service*"),
              hostServiceSection(ss.serviceInfo.serviceParams),
              JuxtaposeSection(
                first = TextField("Up Time", took(ss.serviceInfo.launchTime, ss.timestamp)),
                second = TextField("Time Zone", ss.serviceInfo.serviceParams.taskParams.zoneId.show)
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
        username = sp.serviceInfo.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.errorColor,
            blocks = List(
              MarkdownSection(
                s":alarm: The service experienced a panic, the *${toOrdinalWords(sp.retryDetails.retriesSoFar + 1L)}* time, $upcoming"),
              hostServiceSection(sp.serviceInfo.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", took(sp.serviceInfo.launchTime, sp.timestamp)),
                TextField("Cummulative Delay", cfg.durationFormatter.format(sp.retryDetails.cumulativeDelay))
              ),
              MarkdownSection(s"*Restart Policy:* ${sp.serviceInfo.serviceParams.retry.policy[F].show}"),
              KeyValueSection("Cause", s"```${abbreviate(sp.error.stackTrace)}```")
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      ))
  }

  private def serviceAlert(sa: ServiceAlert): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      val (users, title, color) = sa.importance match {
        case Importance.Critical => (cfg.atSupporters, ":warning: Error", cfg.errorColor)
        case Importance.High     => ("", ":warning: Warning", cfg.warnColor)
        case Importance.Medium   => ("", ":information_source: Info", cfg.infoColor)
        case Importance.Low      => (cfg.atSupporters, "oops. should not happen", cfg.errorColor)
      }
      SlackApp(
        username = sa.serviceInfo.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = color,
            blocks = List(
              MarkdownSection(s"*$title:* ${sa.metricName.value} $users"),
              hostServiceSection(sa.serviceInfo.serviceParams)) :::
              (if (sa.message.isEmpty) Nil else List(MarkdownSection(abbreviate(sa.message))))
          )
        ) :+ Attachment(color = cfg.infoColor, blocks = extra)
      )
    }

  private def serviceStopped(ss: ServiceStopped): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      SlackApp(
        username = ss.serviceInfo.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.warnColor,
            blocks = List(
              MarkdownSection(s":octagonal_sign: *Service Stopped*. ${cfg.atSupporters}"),
              hostServiceSection(ss.serviceInfo.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", took(ss.serviceInfo.launchTime, ss.timestamp)),
                TextField("Time Zone", ss.serviceInfo.serviceParams.taskParams.zoneId.show)),
              metricsSection(ss.snapshot)
            )
          ),
          Attachment(color = cfg.infoColor, blocks = extra)
        )
      )
    }

  private def metricsReport(mr: MetricsReport): F[Option[SlackApp]] = {
    val msg = cfg.extraSlackSections.map { extra =>
      val next = nextTime(
        mr.serviceInfo.serviceParams.metric.reportSchedule,
        mr.timestamp,
        cfg.reportInterval,
        mr.serviceInfo.launchTime).fold("None")(_.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)

      SlackApp(
        username = mr.serviceInfo.serviceParams.taskParams.appName,
        attachments = List(
          Attachment(
            color = cfg.infoColor,
            blocks = List(
              MarkdownSection(s"${cfg.metricsReportEmoji} *${mr.reportType.show}*"),
              hostServiceSection(mr.serviceInfo.serviceParams),
              JuxtaposeSection(
                TextField("Up Time", took(mr.serviceInfo.launchTime, mr.timestamp)),
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
        mr.serviceInfo.serviceParams.metric.reportSchedule,
        mr.timestamp,
        cfg.reportInterval,
        mr.serviceInfo.launchTime) || mr.reportType.isShow

    msg.map(m => if (isShow) Some(m) else None)
  }

  private def metricsReset(mr: MetricsReset): F[SlackApp] =
    cfg.extraSlackSections.map { extra =>
      mr.resetType match {
        case MetricResetType.AdventiveReset =>
          SlackApp(
            username = mr.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection("*Adventive Metrics Reset*"),
                  hostServiceSection(mr.serviceInfo.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", took(mr.serviceInfo.launchTime, mr.timestamp)),
                    TextField(
                      "Scheduled Next",
                      mr.serviceInfo.serviceParams.metric.resetSchedule
                        .flatMap(_.next(mr.timestamp.toInstant))
                        .map(_.atZone(mr.serviceInfo.serviceParams.taskParams.zoneId).toLocalTime
                          .truncatedTo(ChronoUnit.SECONDS)
                          .show)
                        .getOrElse("None")
                    )
                  ),
                  metricsSection(mr.snapshot)
                )
              ),
              Attachment(color = cfg.infoColor, blocks = extra)
            )
          )
        case MetricResetType.ScheduledReset(next) =>
          SlackApp(
            username = mr.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.infoColor,
                blocks = List(
                  MarkdownSection(s"*Scheduled Metrics Reset*"),
                  hostServiceSection(mr.serviceInfo.serviceParams),
                  JuxtaposeSection(
                    TextField("Up Time", took(mr.serviceInfo.launchTime, mr.timestamp)),
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

  private def actionStart(as: ActionStart): F[Option[SlackApp]] =
    cfg.extraSlackSections.map { _ =>
      if (as.actionParams.importance === Importance.Critical)
        Some(
          SlackApp(
            username = as.serviceParams.taskParams.appName,
            attachments = List(Attachment(
              color = cfg.infoColor,
              blocks = List(
                MarkdownSection(
                  s"${cfg.startActionEmoji} Kick off ${as.actionParams.alias}: *${as.metricName.value}*".stripMargin),
                MarkdownSection(s"""|*${as.actionParams.alias} ID:* ${as.uuid.show}""".stripMargin),
                hostServiceSection(as.actionInfo.serviceInfo.serviceParams)
              )
            ))
          ))
      else None
    }

  private def actionRetrying(ar: ActionRetrying): F[Option[SlackApp]] =
    cfg.extraSlackSections.map { _ =>
      if (ar.actionParams.importance >= Importance.Medium) {
        val header: String =
          s"${cfg.retryActionEmoji} This is the *${toOrdinalWords(ar.willDelayAndRetry.retriesSoFar + 1L)}* " +
            s"failure of the ${ar.actionParams.alias} *${ar.actionParams.metricName.value}*, " +
            s"took *${took(ar.launchTime, ar.timestamp)}* so far, " +
            s"retry of which takes place in *${cfg.durationFormatter.format(ar.willDelayAndRetry.nextDelay)}*."

        Some(
          SlackApp(
            username = ar.serviceParams.taskParams.appName,
            attachments = List(Attachment(
              color = cfg.warnColor,
              blocks = List(
                MarkdownSection(header),
                MarkdownSection(s"""|*${ar.actionParams.alias} ID:* ${ar.uuid.show}
                                    |*policy:* ${ar.actionParams.retry.policy[F].show}""".stripMargin),
                hostServiceSection(ar.serviceInfo.serviceParams),
                KeyValueSection("Cause", s"```${abbreviate(ar.error.stackTrace)}```")
              )
            ))
          ))
      } else None
    }

  private def actionFailed(af: ActionFailed): F[Option[SlackApp]] =
    cfg.extraSlackSections.map { extra =>
      if (af.actionParams.importance >= Importance.Medium) {
        val header =
          s"${cfg.failActionEmoji} The ${af.actionParams.alias} *${af.metricName.value}* " +
            s"was failed after *${af.numRetries.show}* retries, " +
            s"took *${took(af.launchTime, af.timestamp)}*. ${cfg.atSupporters}"

        Some(
          SlackApp(
            username = af.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.errorColor,
                blocks = List(
                  MarkdownSection(header),
                  MarkdownSection(s"""|*${af.actionParams.alias} ID:* ${af.uuid.show}
                                      |*error ID:* ${af.actionInfo.uuid.show}
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

  private def actionSucced(as: ActionSucced): F[Option[SlackApp]] =
    cfg.extraSlackSections.map { _ =>
      if (as.actionParams.importance === Importance.Critical) {
        val header =
          s"${cfg.succActionEmoji} The ${as.actionParams.alias} *${as.actionParams.metricName.value}* " +
            s"was accomplished in *${took(as.launchTime, as.timestamp)}*, after *${as.numRetries.show}* retries"

        Some(
          SlackApp(
            username = as.serviceInfo.serviceParams.taskParams.appName,
            attachments = List(
              Attachment(
                color = cfg.goodColor,
                blocks = List(
                  MarkdownSection(header),
                  MarkdownSection(s"*${as.actionParams.alias} ID:* ${as.uuid.show}"),
                  hostServiceSection(as.serviceInfo.serviceParams)
                ) ::: (if (as.notes.value.isEmpty) Nil
                       else List(MarkdownSection(abbreviate(as.notes.value))))
              )
            )
          ))
      } else None
    }

  def translator: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStarted(serviceStarted)
      .withServicePanic(servicePanic)
      .withServiceStopped(serviceStopped)
      .withMetricsReport(metricsReport)
      .withMetricsReset(metricsReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetrying(actionRetrying)
      .withActionFailed(actionFailed)
      .withActionSucced(actionSucced)
}
