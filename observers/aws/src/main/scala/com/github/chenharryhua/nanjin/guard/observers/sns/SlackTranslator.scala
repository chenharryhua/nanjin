package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.{AlertLevel, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.textConstants.*
import com.github.chenharryhua.nanjin.guard.translator.textHelper.*
import com.github.chenharryhua.nanjin.guard.translator.{fmt, ColorScheme, SnapshotPolyglot, Translator}
import io.circe.Json
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.all
import squants.information.{Bytes, Information}

private object SlackTranslator extends all {
  import NJEvent.*

  private def coloring(evt: NJEvent): String = ColorScheme
    .decorate(evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now("#36a64f")
      case ColorScheme.InfoColor  => Eval.now("#b3d1ff")
      case ColorScheme.WarnColor  => Eval.now("#ffd79a")
      case ColorScheme.ErrorColor => Eval.now("#935252")
    }
    .value

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  private val MessageSizeLimits: Information = Bytes(2500)

  private def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MessageSizeLimits.toBytes.toInt)
  private def abbreviate(msg: Json): String   = abbreviate(msg.spaces2)

  private def host_service_section(sp: ServiceParams): JuxtaposeSection = {
    val sn: String =
      sp.homePage.fold(sp.serviceName.value)(hp => s"<${hp.value}|${sp.serviceName.value}>")
    JuxtaposeSection(TextField(CONSTANT_SERVICE, sn), TextField(CONSTANT_HOST, hostText(sp)))
  }

  private def uptime_section(evt: NJEvent): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_UPTIME, uptimeText(evt)),
      second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.zoneId.show))

  private def metrics_section(snapshot: MetricSnapshot): KeyValueSection = {
    val yaml = new SnapshotPolyglot(snapshot).counterYaml match {
      case Some(value) => s"""```${abbreviate(value)}```"""
      case None        => "`No updates`"
    }
    KeyValueSection(CONSTANT_METRICS, yaml)
  }

  private def brief(json: Json): KeyValueSection =
    KeyValueSection(CONSTANT_BRIEF, s"```${abbreviate(json.spaces2)}```")

  private def stack_trace(err: NJError): String =
    abbreviate(err.stack.mkString("\n\t"))

// events
  private def service_started(evt: ServiceStart): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s":rocket: *${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(show"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart}
                                   |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId}""".stripMargin)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def service_panic(evt: ServicePanic): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(":alarm:" + panicText(evt)),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(show"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart}
                                   |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId}""".stripMargin)
          )
        ),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${stack_trace(evt.error)}```"))),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def service_stopped(evt: ServiceStop): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s":octagonal_sign: *${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(show"""|*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId}
                                   |*$CONSTANT_CAUSE:* ${abbreviate(stopCause(evt.cause))}""".stripMargin)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def metric_report(evt: MetricReport): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(show"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId}"),
            metrics_section(evt.snapshot)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def metric_reset(evt: MetricReset): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"),
            metrics_section(evt.snapshot)
          )
        ))
    )

  private def measurement(mn: MetricName): String = s"*$CONSTANT_MEASUREMENT:* ${mn.measurement}"

  private def service_alert(evt: ServiceAlert): SlackApp = {
    val symbol: String = evt.alertLevel match {
      case AlertLevel.Error => ":warning:"
      case AlertLevel.Warn  => ":warning:"
      case AlertLevel.Info  => ":information_source:"
    }
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(symbol + s" *${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(s"""|${measurement(evt.metricName)}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}""".stripMargin),
            MarkdownSection(s"```${abbreviate(evt.message)}```")
          )
        ))
    )
  }

  private def service_id(evt: ActionEvent): String =
    s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"

  private def policy(evt: ActionEvent): String = s"*$CONSTANT_POLICY:* ${evt.actionParams.retryPolicy}"

  private def action_start(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_MEASUREMENT, evt.actionParams.metricName.measurement),
              second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.zoneId.show)),
            MarkdownSection(s"""|${policy(evt)}
                                |${service_id(evt)}""".stripMargin),
            MarkdownSection(s"""```${abbreviate(evt.notes)}```""")
          )
        ))
    )

  private def action_retrying(evt: ActionRetry): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_MEASUREMENT, evt.actionParams.metricName.measurement),
              second = TextField(CONSTANT_SNOOZE, fmt.format(evt.tick.snooze))),
            MarkdownSection(s"""|${retryText(evt)}
                                |${policy(evt)}
                                |${service_id(evt)}""".stripMargin),
            KeyValueSection(CONSTANT_CAUSE, s"""```${abbreviate(evt.error.message)}```""")
          )
        ))
    )

  private def action_failed(evt: ActionFail): SlackApp = {
    val color: String = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_MEASUREMENT, evt.actionParams.metricName.measurement),
              second = TextField(CONSTANT_TOOK, tookText(evt.took))),
            MarkdownSection(s"""|${policy(evt)}
                                |${service_id(evt)}""".stripMargin),
            MarkdownSection(s"""```${abbreviate(evt.notes)}```""")
          )
        ),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${stack_trace(evt.error)}```"))),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def action_done(evt: ActionDone): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            host_service_section(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_MEASUREMENT, evt.actionParams.metricName.measurement),
              second = TextField(CONSTANT_TOOK, tookText(evt.took))),
            MarkdownSection(service_id(evt)),
            MarkdownSection(s"""```${abbreviate(evt.notes)}```""")
          )
        )
      )
    )

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(service_started)
      .withServicePanic(service_panic)
      .withServiceStop(service_stopped)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceAlert(service_alert)
      .withActionStart(action_start)
      .withActionRetry(action_retrying)
      .withActionFail(action_failed)
      .withActionDone(action_done)
}
