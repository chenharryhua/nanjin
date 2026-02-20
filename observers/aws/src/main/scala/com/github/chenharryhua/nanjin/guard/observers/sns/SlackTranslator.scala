package com.github.chenharryhua.nanjin.guard.observers.sns
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.show.{showInterpolator, toShow}
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, Attribute, Brief, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{Active, Event, Index, MetricSnapshot, Snooze}
import com.github.chenharryhua.nanjin.guard.translator.textHelper.*
import com.github.chenharryhua.nanjin.guard.translator.{ColorScheme, SnapshotPolyglot, Translator}
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

  private def host_service_section(sp: ServiceParams): JuxtaposeSection = {
    val host = Attribute(sp.host).textEntry
    val service =
      Attribute(sp.serviceName).textEntry(name =>
        sp.homepage.fold(name.value)(hp => s"<${hp.value}|${name.value}>"))
    JuxtaposeSection(TextField(service), TextField(host))
  }

  private def uptime_section(evt: Event): JuxtaposeSection = {
    val uptime = Attribute(evt.upTime).textEntry
    val zone = Attribute(evt.serviceParams.timeZone).textEntry
    JuxtaposeSection(first = TextField(uptime), second = TextField(zone))
  }

  private def metrics_index_section(evt: MetricsEvent): JuxtaposeSection = {
    val uptime = Attribute(evt.upTime).textEntry
    val index = Attribute(evt.index).textEntry
    JuxtaposeSection(first = TextField(uptime), second = TextField(index))
  }

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
      KeyValueSection("Metrics", s"""```$msg```""")
    } else KeyValueSection("Metrics", """`not available`""")

  private def brief(sb: Brief): KeyValueSection = {
    val service_brief = Attribute(sb).textEntry
    KeyValueSection(service_brief.tag, s"```${abbreviate(service_brief.text)}```")
  }

  // events
  private def service_start(evt: ServiceStart): SlackApp = {
    val zone = Attribute(evt.serviceParams.timeZone).textEntry
    val index = Attribute(Index(evt.tick.index)).textEntry
    val snooze = Attribute(Snooze(evt.tick.snooze)).textEntry

    val index_section = if (evt.tick.index === 0) {
      JuxtaposeSection(first = TextField(zone), second = TextField(index))
    } else {
      JuxtaposeSection(first = TextField(snooze), second = TextField(index))
    }

    val color = coloring(evt)
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).textEntry
    val service_id = Attribute(evt.serviceParams.serviceId).textEntry
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":rocket: ${eventTitle(evt)}"),
            host_service_section(evt.serviceParams),
            index_section,
            MarkdownSection(show"""|*${policy.tag}:* ${policy.text}
                                   |*${service_id.tag}:* ${service_id.text}""".stripMargin)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def service_panic(evt: ServicePanic): SlackApp = {
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val service_id = Attribute(evt.serviceParams.serviceId).textEntry
    val index = Attribute(Index(evt.tick.index)).textEntry
    val error = Attribute(evt.stackTrace).textEntry
    val active = Attribute(Active(evt.tick.active)).textEntry

    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":alarm: ${eventTitle(evt)}"),
            host_service_section(evt.serviceParams),
            JuxtaposeSection(first = TextField(active), second = TextField(index)),
            MarkdownSection(show"""|${panicText(evt)}
                                   |*${uptime.tag}:* ${uptime.text}
                                   |*${policy.tag}:* ${policy.text}
                                   |*${service_id.tag}:* ${service_id.text}""".stripMargin)
          )
        ),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(error.tag, s"```${abbreviate(error.text)}```"))),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def service_stop(evt: ServiceStop): SlackApp = {
    val color = coloring(evt)
    val service_id = Attribute(evt.serviceParams.serviceId).textEntry
    val stop_cause = Attribute(evt.cause).textEntry

    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":octagonal_sign: ${eventTitle(evt)}"),
            host_service_section(evt.serviceParams),
            uptime_section(evt),
            MarkdownSection(show"""|*${service_id.tag}:* ${service_id.text}
                                   |*${stop_cause.tag}:* ${stop_cause.text}""".stripMargin)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def metrics_report(evt: MetricsReport): SlackApp = {
    val policy = Attribute(evt.serviceParams.servicePolicies.metricsReport).textEntry
    val service_id = Attribute(evt.serviceParams.serviceId).textEntry
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
            MarkdownSection(show"""|*${policy.tag}:* ${policy.text}
                                   |*${service_id.tag}:* ${service_id.text}""".stripMargin),
            metrics_section(evt.snapshot)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def metrics_reset(evt: MetricsReset): SlackApp = {
    val policy = Attribute(evt.serviceParams.servicePolicies.metricsReset).textEntry
    val service = Attribute(evt.serviceParams.serviceId).textEntry

    SlackApp(
      username = evt.serviceParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceParams),
            metrics_index_section(evt),
            MarkdownSection(show"""|*${policy.tag}:* ${policy.text}
                                   |*${service.tag}:* ${service.text}""".stripMargin),
            metrics_section(evt.snapshot)
          )
        ))
    )
  }

  private def service_message(evt: ServiceMessage): SlackApp = {
    val symbol: String = evt.level match {
      case AlarmLevel.Error => ":warning:"
      case AlarmLevel.Warn  => ":warning:"
      case AlarmLevel.Info  => ""
      case AlarmLevel.Done  => ""
      case AlarmLevel.Debug => ""
    }

    val color = coloring(evt)
    val domain = Attribute(evt.domain).textEntry
    val service = Attribute(evt.serviceParams.serviceId).textEntry
    val correlation = Attribute(evt.correlation).textEntry

    val attachment = Attachment(
      color = color,
      blocks = List(
        HeaderSection(s"$symbol ${eventTitle(evt)}"),
        host_service_section(evt.serviceParams),
        JuxtaposeSection(TextField(domain), TextField(correlation)),
        MarkdownSection(s"*${service.tag}:* ${service.text}"),
        MarkdownSection(s"```${abbreviate(evt.message.show)}```")
      )
    )

    val error = evt.stackTrace.map { err =>
      val reason = Attribute(err).textEntry
      Attachment(
        color = color,
        blocks = List(KeyValueSection(reason.tag, s"```${abbreviate(reason.text)}```")))
    }

    SlackApp(username = evt.serviceParams.taskName.value, attachments = List(Some(attachment), error).flatten)
  }

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(service_start)
      .withServicePanic(service_panic)
      .withServiceStop(service_stop)
      .withMetricsReport(metrics_report)
      .withMetricsReset(metrics_reset)
      .withServiceMessage(service_message)
}
