package com.github.chenharryhua.nanjin.guard.observers.teams

import cats.syntax.traverse.given
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.{Active, Event, Snooze}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{IndentSpace, SnapshotPolyglot}
import com.github.chenharryhua.nanjin.guard.observers.CloudWatchLogs
import com.github.chenharryhua.nanjin.guard.translator.{
  eventLogLevel,
  eventTitle,
  panicText,
  Attribute,
  Translator
}

private object TeamsTranslator {
  import Event.*

  private case class Index(value: Long)

  private def coloring(evt: Event): String =
    eventLogLevel[Eval, String](evt).run {
      case LogLevel.Good  => Eval.now("Good")
      case LogLevel.Info  => Eval.now("Default")
      case LogLevel.Warn  => Eval.now("Warning")
      case LogLevel.Error => Eval.now("Attention")
      case LogLevel.Debug => Eval.now("Dark")
    }.value

  private def headerBlock(evt: Event): TextBlock = {
    val symbol = eventLogLevel[Eval, String](evt).run {
      case LogLevel.Good  => Eval.now("\u2705")
      case LogLevel.Info  => Eval.now("\u2139\uFE0F")
      case LogLevel.Warn  => Eval.now("\u26A0\uFE0F")
      case LogLevel.Error => Eval.now("\u274C")
      case LogLevel.Debug => Eval.now("\uD83D\uDD0D")
    }.value
    TextBlock(
      s"$symbol ${eventTitle(evt)}",
      color = coloring(evt),
      weight = Some("Bolder"),
      size = Some("Medium"))
  }

  private def serviceInfo(evt: Event): FactSet = {
    val sp = evt.serviceParams
    val service = Attribute(sp.serviceName).textEntry
    val host = Attribute(sp.host).textEntry
    val sid = Attribute(sp.serviceId).textEntry
    val ts = Attribute(evt.timestamp).textEntry
    FactSet(
      List(
        Fact(ts.tag, ts.text),
        Fact(service.tag, sp.homepage.fold(service.text)(hp => s"[${service.text}]($hp)")),
        Fact(host.tag, host.text),
        Fact(sid.tag, sid.text)
      ))
  }

  private def service_start(evt: ServiceStart): AdaptiveCard = {
    val snz = Attribute(Snooze(evt.tick.snooze)).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val idx = Attribute(Index(evt.tick.index)).map(_.value).textEntry
    val brief = Attribute(evt.serviceParams.brief).typeName

    AdaptiveCard(
      body = List(
        headerBlock(evt),
        serviceInfo(evt),
        FactSet(
          List(
            Fact(idx.tag, idx.text),
            Fact(snz.tag, snz.text),
            Fact(uptime.tag, uptime.text)
          )),
        BolderTextBlock(brief),
        JsonBlock(evt.serviceParams.brief.value)
      )
    )
  }

  private def service_panic(evt: ServicePanic): AdaptiveCard = {
    val active = Attribute(Active(evt.tick.active)).textEntry
    val policy = Attribute(evt.serviceParams.policies.restart.policy).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val idx = Attribute(Index(evt.tick.index)).map(_.value).textEntry
    val stackTrace = Attribute(evt.stackTrace).typeName
    val brief = Attribute(evt.serviceParams.brief).typeName

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
        TextBlock(panicText(evt), color = "Attention"),
        BolderTextBlock(stackTrace),
        StackTraceBlock(evt.stackTrace),
        BolderTextBlock(brief),
        JsonBlock(evt.serviceParams.brief.value)
      )
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
      )
    )
  }

  private def metrics_snapshot(evt: MetricsSnapshot): AdaptiveCard = {
    val policy = Attribute(evt.serviceParams.policies.report).textEntry
    val idx = Attribute(evt.index).textEntry
    val snapshot = Attribute(evt.snapshot).typeName
    val yaml = new SnapshotPolyglot(evt.snapshot, IndentSpace.Nbsp).toYaml

    AdaptiveCard(
      body = List(
        headerBlock(evt),
        serviceInfo(evt),
        FactSet(
          List(
            Fact(idx.tag, idx.text),
            Fact(policy.tag, policy.text)
          )),
        BolderTextBlock(snapshot),
        TextBlock(yaml)
      )
    )
  }

  private def reported_event(evt: ReportedEvent): AdaptiveCard = {
    val domain = Attribute(evt.domain).textEntry
    val correlation = Attribute(evt.correlation).textEntry
    val message = Attribute(evt.message).typeName
    val stackTrace = evt.stackTrace.map { st =>
      val attr = Attribute(st).typeName
      List(BolderTextBlock(attr), StackTraceBlock(st))
    }.sequence.flatten

    val body = List(
      headerBlock(evt),
      serviceInfo(evt),
      FactSet(
        List(
          Fact(domain.tag, domain.text),
          Fact(correlation.tag, correlation.text)
        )),
      BolderTextBlock(message),
      JsonBlock(evt.message.value)
    ) ++ stackTrace

    val card = AdaptiveCard(body)

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
