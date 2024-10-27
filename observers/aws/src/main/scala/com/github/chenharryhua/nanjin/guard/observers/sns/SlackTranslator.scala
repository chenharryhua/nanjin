package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.metricConstants.METRICS_DIGEST
import com.github.chenharryhua.nanjin.guard.translator.textConstants.*
import com.github.chenharryhua.nanjin.guard.translator.textHelper.*
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, SnapshotPolyglot, Translator}
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

  private def name_section(mn: MetricName): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_MEASUREMENT, mn.measurement),
      second = TextField(CONSTANT_NAME, mn.name)
    )

  private def metrics_index_section(evt: MetricEvent): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_UPTIME, uptimeText(evt)),
      second = TextField(CONSTANT_INDEX, metricIndexText(evt.index)))

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
    val index_section = if (evt.tick.index == 0) {
      JuxtaposeSection(
        first = TextField(CONSTANT_TIMEZONE, evt.serviceParams.zoneId.show),
        second = TextField(CONSTANT_INDEX, evt.tick.index.show)
      )
    } else {
      JuxtaposeSection(
        first = TextField(CONSTANT_SNOOZED, tookText(evt.tick.snooze)),
        second = TextField(CONSTANT_INDEX, evt.tick.index.show)
      )
    }

    val color = coloring(evt)

    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":rocket: ${eventTitle(evt)}"),
            host_service_section(evt.serviceParams),
            index_section,
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
            HeaderSection(s":alarm: ${eventTitle(evt)}"),
            host_service_section(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTIVE, tookText(evt.tick.active)),
              second = TextField(CONSTANT_INDEX, evt.tick.index.show)
            ),
            MarkdownSection(show"""|${panicText(evt)}
                                   |*$CONSTANT_UPTIME:* ${uptimeText(evt)}
                                   |*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart}
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
            HeaderSection(s":octagonal_sign: ${eventTitle(evt)}"),
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
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            metrics_index_section(evt),
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
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            metrics_index_section(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"),
            metrics_section(evt.snapshot)
          )
        ))
    )

  private def service_alert(evt: ServiceMessage): SlackApp = {
    val symbol: String = evt.level match {
      case AlarmLevel.Error => ":warning:"
      case AlarmLevel.Warn  => ":warning:"
      case AlarmLevel.Info  => ""
      case AlarmLevel.Done  => ""
    }
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            HeaderSection(s"$symbol ${eventTitle(evt)}"),
            host_service_section(evt.serviceParams),
            name_section(evt.metricName),
            MarkdownSection(s"""|*$METRICS_DIGEST:* ${evt.metricName.digest}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}""".stripMargin),
            MarkdownSection(s"```${abbreviate(evt.message)}```")
          )
        ))
    )
  }

  private def service_id(evt: ActionEvent): String =
    s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"

  private def action_digest(evt: ActionEvent): String =
    s"*$METRICS_DIGEST:* ${evt.actionParams.metricName.digest}"

  private def policy(evt: ActionEvent): String =
    show"*$CONSTANT_POLICY:* ${evt.actionParams.retryPolicy}"

  private def action_start(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            name_section(evt.actionParams.metricName),
            MarkdownSection(s"""|${policy(evt)}
                                |${action_digest(evt)}
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
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            name_section(evt.actionParams.metricName),
            MarkdownSection(s"""|${retryText(evt)}
                                |${policy(evt)}
                                |${action_digest(evt)}
                                |${service_id(evt)}
                                |*$CONSTANT_CAUSE:* ${evt.error.message}""".stripMargin),
            MarkdownSection(s"""```${abbreviate(evt.notes)}```""")
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
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            name_section(evt.actionParams.metricName),
            MarkdownSection(s"""*$CONSTANT_TOOK:* ${tookText(evt.took)}
                               |${policy(evt)}
                               |${action_digest(evt)}
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
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            name_section(evt.actionParams.metricName),
            MarkdownSection(s"""|*$CONSTANT_TOOK:* ${tookText(evt.took)}
                                |${action_digest(evt)}
                                |${service_id(evt)}""".stripMargin),
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
      .withServiceMessage(service_alert)
      .withActionStart(action_start)
      .withActionRetry(action_retrying)
      .withActionFail(action_failed)
      .withActionDone(action_done)
}
