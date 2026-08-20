package com.github.chenharryhua.nanjin.guard.observers.teams

import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.{Active, Event, Snooze}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.SnapshotPolyglot
import com.github.chenharryhua.nanjin.guard.observers.CloudWatchLogs
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  eventTitle,
  panicText,
  Attribute,
  Translator
}

private[teams] object TeamsTranslator {
  import Event.*

  private case class Index(value: Long)

  private def coloring(evt: Event): String =
    eventLogLevel[Eval, String](evt).run {
      case LogLevel.Good  => Eval.now("00FF00")
      case LogLevel.Info  => Eval.now("0078D4")
      case LogLevel.Warn  => Eval.now("FFA500")
      case LogLevel.Error => Eval.now("FF0000")
      case LogLevel.Debug => Eval.now("800080")
    }.value

  private def headerBlock(evt: Event): TextBlock = {
    val symbol = eventLogLevel[Eval, String](evt).run {
      case LogLevel.Good  => Eval.now("\u2705")
      case LogLevel.Info  => Eval.now("\u2139\uFE0F")
      case LogLevel.Warn  => Eval.now("\u26A0\uFE0F")
      case LogLevel.Error => Eval.now("\u274C")
      case LogLevel.Debug => Eval.now("\uD83D\uDD0D")
    }.value
    TextBlock(s"$symbol ${eventTitle(evt)}", weight = Some("Bolder"), size = Some("Medium"))
  }

  private def serviceInfo(evt: Event): FactSet = {
    val sp = evt.serviceParams
    val service = Attribute(sp.serviceName).textEntry
    val host = Attribute(sp.host).textEntry
    val sid = Attribute(sp.serviceId).textEntry
    val ts = Attribute(evt.timestamp).textEntry
    FactSet(List(
      Fact(ts.tag, ts.text),
      Fact(service.tag, service.text),
      Fact(host.tag, host.text),
      Fact(sid.tag, sid.text)
    ))
  }

  private def service_start(evt: ServiceStart): AdaptiveCard = {
    val snz = Attribute(Snooze(evt.tick.snooze)).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val idx = Attribute(Index(evt.tick.index)).map(_.value).textEntry

    AdaptiveCard(
      body = List(
        headerBlock(evt),
        serviceInfo(evt),
        FactSet(
          List(
            Fact(idx.tag, idx.text),
            Fact(snz.tag, snz.text),
            Fact(uptime.tag, uptime.text)
          ))
      ),
      themeColor = coloring(evt)
    )
  }

  private def service_panic(evt: ServicePanic): AdaptiveCard = {
    val active = Attribute(Active(evt.tick.active)).textEntry
    val policy = Attribute(evt.serviceParams.policies.restart.policy).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val error = Attribute(evt.stackTrace).textEntry
    val idx = Attribute(Index(evt.tick.index)).map(_.value).textEntry

    val card = AdaptiveCard(
      body = List(
        headerBlock(evt),
        serviceInfo(evt),
        FactSet(
          List(
            Fact(idx.tag, idx.text),
            Fact(active.tag, active.text),
            Fact(policy.tag, policy.text),
            Fact(uptime.tag, uptime.text)
          )),
        TextBlock(panicText(evt), color = Some("Attention")),
        CodeBlock(error.text)
      ),
      themeColor = coloring(evt)
    )

    CloudWatchLogs.logLink(evt.serviceParams.brief, evt.timestamp.value.toInstant)
      .map(url => TextBlock(s"[\uD83D\uDD0D CloudWatch Logs]($url)"))
      .fold(card)(card.appendElement)
  }

  private def service_stop(evt: ServiceStop): AdaptiveCard = {
    val cause = Attribute(evt.cause).textEntry
    val uptime = Attribute(evt.upTime).textEntry

    AdaptiveCard(
      body = List(
        headerBlock(evt),
        serviceInfo(evt),
        FactSet(
          List(
            Fact(cause.tag, cause.text),
            Fact(uptime.tag, uptime.text)
          ))
      ),
      themeColor = coloring(evt)
    )
  }

  private def metrics_snapshot(evt: MetricsSnapshot): AdaptiveCard = {
    val policy = Attribute(evt.serviceParams.policies.report).textEntry
    val idx = Attribute(evt.index).textEntry
    val yaml = new SnapshotPolyglot(evt.snapshot).toYaml

    AdaptiveCard(
      body = List(
        headerBlock(evt),
        serviceInfo(evt),
        FactSet(
          List(
            Fact(idx.tag, idx.text),
            Fact(policy.tag, policy.text)
          )),
        CodeBlock(yaml, language = "yaml")
      ),
      themeColor = coloring(evt)
    )
  }

  private def reported_event(evt: ReportedEvent): AdaptiveCard = {
    val domain = Attribute(evt.domain).textEntry
    val correlation = Attribute(evt.correlation).textEntry
    val message = evt.message.value.spaces2

    val body = List(
      headerBlock(evt),
      serviceInfo(evt),
      FactSet(
        List(
          Fact(domain.tag, domain.text),
          Fact(correlation.tag, correlation.text)
        )),
      CodeBlock(message, language = "json")
    ) ++ evt.stackTrace.map(st => CodeBlock(Attribute(st).textEntry.text))

    val card = AdaptiveCard(body = body, themeColor = coloring(evt))

    CloudWatchLogs.logLink(evt.serviceParams.brief, evt.timestamp.value.toInstant)
      .map(url => TextBlock(s"[\uD83D\uDD0D CloudWatch Logs]($url)"))
      .fold(card)(card.appendElement)
  }

  def apply[F[_]: Applicative]: Translator[F, AdaptiveCard] =
    Translator
      .empty[F, AdaptiveCard]
      .withServiceStart(service_start)
      .withServicePanic(service_panic)
      .withServiceStop(service_stop)
      .withMetricsSnapshot(metrics_snapshot)
      .withReportedEvent(reported_event)
}
