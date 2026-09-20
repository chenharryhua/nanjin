package com.github.chenharryhua.nanjin.guard.observers.slack

import cats.syntax.order.given
import cats.syntax.show.{showInterpolator, given}
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.common.logging.{LogLevel, LogLink}
import com.github.chenharryhua.nanjin.guard.config.{Brief, ServiceIdentity, StackTrace}
import com.github.chenharryhua.nanjin.guard.event.{Active, Correlation, Event, Snooze}
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  eventTitle,
  panicText,
  Attribute,
  SnapshotPolyglot,
  TextEntry,
  Translator
}
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.all
import squants.information.{Bytes, Information}

private object SlackTranslator extends all {
  import Event.*

  private case class Index(value: Long)

  private def coloring(evt: Event): String =
    eventLogLevel[Eval, String](evt)
      .run {
        case LogLevel.Good  => Eval.now("#36a64f")
        case LogLevel.Info  => Eval.now("#b3d1ff")
        case LogLevel.Warn  => Eval.now("#ffd79a")
        case LogLevel.Error => Eval.now("#935252")
        case LogLevel.Debug => Eval.now("#FF00FF")
      }
      .value

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  private val MESSAGE_SIZE_LIMIT: Information = Bytes(2500)

  private def escape(str: String): String =
    str.replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")

  private def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MESSAGE_SIZE_LIMIT.toBytes.toInt)

  private def mark_down(first: TextEntry, second: TextEntry): MarkdownSection =
    MarkdownSection(s"""|*${first.tag}:* ${first.text}
                        |*${second.tag}:* ${second.text}""".stripMargin)

  private def single_field(entry: TextEntry): MarkdownSection =
    MarkdownSection(s"*${entry.tag}:* ${entry.text}")

  private def host_service_section(sp: ServiceIdentity): JuxtaposeSection = {
    val host = Attribute(sp.host).textEntry
    val (tag, name) = Attribute(sp.service).textEntry.withText(escape).toPair
    val service = sp.homepage match {
      case Some(value) => TextField(tag, s"<$value|$name>")
      case None        => TextField(tag, name)
    }
    JuxtaposeSection(service, TextField(host))
  }

  private def uptime_section(evt: Event): JuxtaposeSection = {
    val uptime = Attribute(evt.upTime).textEntry
    val zone = Attribute(evt.serviceIdentity.timeZone).textEntry
    JuxtaposeSection(first = TextField(uptime), second = TextField(zone))
  }

  private def metrics_index_section(evt: MetricsSnapshot): JuxtaposeSection = {
    val uptime = Attribute(evt.upTime).textEntry
    val idx = Attribute(evt.index).textEntry
    JuxtaposeSection(first = TextField(idx), second = TextField(uptime))
  }

  private def metrics_section(evt: MetricsSnapshot): TagValueSection = {
    val ss = Attribute(evt.snapshot).map(new SnapshotPolyglot(_).toYaml).textEntry
    val tag = evt.logLink.fold(ss.tag)(link => s"<${link.value}|${ss.tag}>")
    if (evt.snapshot.nonEmpty) {
      TagValueSection(tag, s"""```${abbreviate(ss.text)}```""")
    } else TagValueSection(tag, """`not available`""")
  }

  private def message_section(evt: ReportedEvent): TagValueSection = {
    val ss = Attribute(evt.message).map(msg => s"```${abbreviate(msg.value.spaces2)}```").textEntry
    val tag = evt.logLink.fold(ss.tag)(link => s"<${link.value}|${ss.tag}>")

    TagValueSection(tag, ss.text)
  }

  private def brief(serviceBrief: Brief, logLink: Option[LogLink]): TagValueSection = {
    val sb = Attribute(serviceBrief).textEntry
    val tag = logLink.fold(sb.tag)(link => s"<${link.value}|${sb.tag}>")
    TagValueSection(tag, s"```${abbreviate(sb.text)}```")
  }

  // events
  private def service_start(evt: ServiceStart): SlackApp = {
    val zone = Attribute(evt.serviceIdentity.timeZone).textEntry
    val index = Attribute(Index(evt.tick.index)).map(_.value).textEntry
    val snooze = Attribute(Snooze(evt.tick.snooze)).textEntry

    val index_section = if (evt.tick.index === 0) {
      JuxtaposeSection(first = TextField(zone), second = TextField(index))
    } else {
      JuxtaposeSection(first = TextField(snooze), second = TextField(index))
    }

    val color = coloring(evt)
    val service_id = Attribute(evt.serviceIdentity.serviceId).textEntry
    SlackApp(
      username = evt.serviceIdentity.task.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":rocket: ${eventTitle(evt)}"),
            host_service_section(evt.serviceIdentity),
            index_section,
            single_field(service_id)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.brief, evt.logLink)))
      )
    )
  }

  private def service_panic(evt: ServicePanic): SlackApp = {
    val uptime = Attribute(evt.upTime).textEntry
    val service_id = Attribute(evt.serviceIdentity.serviceId).textEntry
    val index = Attribute(Index(evt.tick.index)).map(_.value).textEntry
    val error = Attribute(evt.stackTrace).textEntry.withText(txt => s"```${abbreviate(txt)}```")
    val active = Attribute(Active(evt.tick.active)).textEntry
    val color = coloring(evt)

    SlackApp(
      username = evt.serviceIdentity.task.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":alarm: ${eventTitle(evt)}"),
            host_service_section(evt.serviceIdentity),
            JuxtaposeSection(first = TextField(active), second = TextField(index)),
            MarkdownSection(show"""|`${panicText(evt)}`
                                   |*${uptime.tag}:* ${uptime.text}
                                   |*${service_id.tag}:* ${service_id.text}""".stripMargin)
          )
        ),
        Attachment(color = color, blocks = List(TagValueSection(error.tag, error.text))),
        Attachment(color = color, blocks = List(brief(evt.brief, evt.logLink)))
      )
    )
  }

  private def service_stop(evt: ServiceStop): SlackApp = {
    val color = coloring(evt)
    val service_id = Attribute(evt.serviceIdentity.serviceId).textEntry
    val stop_cause = Attribute(evt.cause).textEntry

    SlackApp(
      username = evt.serviceIdentity.task.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(s":octagonal_sign: ${eventTitle(evt)}"),
            host_service_section(evt.serviceIdentity),
            uptime_section(evt),
            mark_down(service_id, stop_cause)
          )
        ),
        Attachment(color = color, blocks = List(brief(evt.brief, evt.logLink)))
      )
    )
  }

  private def metrics_snapshot(evt: MetricsSnapshot): SlackApp = {
    val service_id = Attribute(evt.serviceIdentity.serviceId).textEntry
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceIdentity.task.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            HeaderSection(eventTitle(evt)),
            host_service_section(evt.serviceIdentity),
            metrics_index_section(evt),
            single_field(service_id),
            metrics_section(evt)
          )
        ))
    )
  }

  private def reported_event(evt: ReportedEvent): SlackApp = {
    val symbol: String = evt.level match {
      case LogLevel.Error => ":x:"
      case LogLevel.Warn  => ":warning:"
      case LogLevel.Info  => ""
      case LogLevel.Good  => ""
      case LogLevel.Debug => ""
    }

    val color = coloring(evt)
    val domain = Attribute(evt.domain).textEntry
    val service = Attribute(evt.serviceIdentity.serviceId).textEntry
    val correlation = Attribute(evt.correlation).textEntry

    val attachment = Attachment(
      color = color,
      blocks = List(
        HeaderSection(s"$symbol ${eventTitle(evt)}"),
        host_service_section(evt.serviceIdentity),
        JuxtaposeSection(TextField(domain), TextField(correlation)),
        MarkdownSection(s"*${service.tag}:* ${service.text}"),
        message_section(evt)
      )
    )

    val error: Option[Attachment] = Attribute(evt.stackTrace).fold { (tag, ost) =>
      ost.map { st =>
        Attachment(color = color, blocks = List(TagValueSection(tag, s"```${abbreviate(st.show)}```")))
      }
    }

    SlackApp(username = evt.serviceIdentity.task.value, attachments = List(Some(attachment), error).flatten)
  }

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(service_start)
      .withServicePanic(service_panic)
      .withServiceStop(service_stop)
      .withMetricsSnapshot(metrics_snapshot)
      .withReportedEvent(reported_event)
}
