package com.github.chenharryhua.nanjin.guard.observers.sns

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, MetricSnapshot, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.textConstants.*
import com.github.chenharryhua.nanjin.guard.translator.textHelper.*
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, SnapshotPolyglot, Translator}
import io.circe.Json
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.all
import squants.information.{Bytes, Information}

private object SlackTranslator extends all {
  import Event.*

  private def coloring(evt: Event): String = ColorScheme
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
  private def abbreviate(msg: Json): String = abbreviate(msg.spaces2)

  private def host_service_section(sp: ServiceParams): JuxtaposeSection = {
    val sn: String =
      sp.homePage.fold(sp.serviceName.value)(hp => s"<${hp.value}|${sp.serviceName.value}>")
    JuxtaposeSection(TextField(CONSTANT_SERVICE, sn), TextField(CONSTANT_HOST, sp.host.toString()))
  }

  private def uptime_section(evt: Event): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_UPTIME, uptimeText(evt)),
      second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.zoneId.show))

  private def metrics_index_section(evt: MetricEvent): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_UPTIME, uptimeText(evt)),
      second = TextField(CONSTANT_INDEX, metricIndexText(evt.index)))

  private def metrics_section(snapshot: MetricSnapshot): KeyValueSection =
    if (snapshot.nonEmpty) {
      val polyglot: SnapshotPolyglot = new SnapshotPolyglot(snapshot)
      val yaml: String = polyglot.toYaml
      val msg: String =
        if (yaml.length < MessageSizeLimits.toBytes.toInt) yaml
        else {
          polyglot.counterYaml match {
            case Some(value) => abbreviate(value)
            case None        => abbreviate(yaml)
          }
        }
      KeyValueSection(CONSTANT_METRICS, s"""```$msg```""")
    } else KeyValueSection(CONSTANT_METRICS, """`not available`""")

  private def brief(json: Json): KeyValueSection =
    KeyValueSection(CONSTANT_BRIEF, s"```${abbreviate(json.spaces2)}```")

  private def stack_trace(err: Error): String =
    abbreviate(err.stack.mkString("\n\t"))

// events
  private def service_start(evt: ServiceStart): SlackApp = {
    val index_section = if (evt.tick.index === 0) {
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
            MarkdownSection(show"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart.policy}
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
                                   |*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart.policy}
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

  private def service_stop(evt: ServiceStop): SlackApp = {
    def stopCause(ssc: ServiceStopCause): String = ssc match {
      case ServiceStopCause.Successfully       => "Successfully"
      case ServiceStopCause.ByCancellation     => "ByCancellation"
      case ServiceStopCause.ByException(error) => s"""```${abbreviate(error.stack.mkString("\n\t"))}```"""
      case ServiceStopCause.Maintenance        => "Maintenance"
    }
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
                                   |*$CONSTANT_CAUSE:* ${stopCause(evt.cause)}""".stripMargin)
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
            MarkdownSection(show"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.metricReport}
                                   |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId}""".stripMargin),
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
            MarkdownSection(show"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.metricReset}
                                   |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId}""".stripMargin),
            metrics_section(evt.snapshot)
          )
        ))
    )

  private def service_message(evt: ServiceMessage): SlackApp = {
    val symbol: String = evt.level match {
      case AlarmLevel.Error => ":warning:"
      case AlarmLevel.Warn  => ":warning:"
      case AlarmLevel.Info  => ""
      case AlarmLevel.Done  => ""
      case AlarmLevel.Debug => ""
    }

    val color = coloring(evt)

    val attachment = Attachment(
      color = color,
      blocks = List(
        HeaderSection(s"$symbol ${eventTitle(evt)}"),
        host_service_section(evt.serviceParams),
        JuxtaposeSection(
          TextField(CONSTANT_DOMAIN, evt.domain.value),
          TextField(CONSTANT_MESSAGE_TOKEN, evt.token.toString)),
        MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"),
        MarkdownSection(s"```${abbreviate(evt.message)}```")
      )
    )

    val error = evt.error.map(err =>
      Attachment(color = color, blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${stack_trace(err)}```"))))

    SlackApp(username = evt.serviceParams.taskName.value, attachments = List(Some(attachment), error).flatten)
  }

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(service_start)
      .withServicePanic(service_panic)
      .withServiceStop(service_stop)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_message)
}
