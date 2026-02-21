package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.Applicative
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.event.{Active, Event, Snooze, StackTrace}
import com.github.chenharryhua.nanjin.guard.translator.{
  eventTitle,
  htmlHelper,
  interpretServiceParams,
  panicText,
  Attribute,
  SnapshotPolyglot,
  Translator
}
import io.circe.Json
import org.typelevel.cats.time.instances.all
import scalatags.Text.all.*
import scalatags.text.Builder
import scalatags.{generic, Text}

/** https://com-lihaoyi.github.io/scalatags/
  */
private object HtmlTranslator extends all {
  import Event.*
  import htmlHelper.*
  private case class Index(value: Long)

  private def service_table(evt: Event): generic.Frag[Builder, String] = {
    val task_name = Attribute(evt.serviceParams.taskName).textEntry
    val host = Attribute(evt.serviceParams.host).textEntry
    val (service_tag, service) = Attribute(evt.serviceParams.serviceName).entry(s =>
      evt.serviceParams.homepage.fold(td(s.value))(hp => td(a(href := hp.value)(s.value))))
    val service_id = Attribute(evt.serviceParams.serviceId).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val timestamp = Attribute(evt.timestamp).textEntry

    frag(
      tr(th(task_name.tag), th(host.tag), th(timestamp.tag)),
      tr(td(task_name.text), td(host.text), td(timestamp.text)),
      tr(th(service_tag), th(service_id.tag), th(uptime.tag)),
      tr(service, td(service_id.text), td(uptime.text))
    )
  }

  private def json_text(js: Json): Text.TypedTag[String] =
    pre(small(js.spaces2))

  private def stack_trace_text(c: StackTrace): Text.TypedTag[String] = {
    val err = Attribute(c).textEntry
    p(b(s"${err.tag}: "), pre(small(err.text)))
  }

  // events

  private def service_start(evt: ServiceStart): Text.TypedTag[String] = {
    val index = Attribute(Index(evt.tick.index)).map(_.value).textEntry
    val active = Attribute(Active(evt.tick.active)).textEntry
    val snooze = Attribute(Snooze(evt.tick.snooze)).textEntry

    val fg = frag(
      tr(th(index.tag), th(active.tag), th(snooze.tag)),
      tr(td(index.text), td(active.text), td(snooze.text))
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      json_text(interpretServiceParams(evt.serviceParams))
    )
  }

  private def service_panic(evt: ServicePanic): Text.TypedTag[String] = {
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).textEntry
    val index = Attribute(Index(evt.tick.index)).map(_.value).textEntry
    val active = Attribute(Active(evt.tick.active)).textEntry

    val fg = frag(
      tr(th(index.tag), th(policy.tag), th(active.tag)),
      tr(td(index.text), td(policy.text), td(active.text))
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      p(b(panicText(evt))),
      stack_trace_text(evt.stackTrace)
    )
  }

  private def service_stop(evt: ServiceStop): Text.TypedTag[String] = {
    val stop_cause = Attribute(evt.cause).textEntry

    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt)),
      p(b(s"${stop_cause.tag}: "), stop_cause.text),
      json_text(evt.serviceParams.brief.value)
    )
  }

  private def metrics_event(evt: MetricsEvent, policy: Policy): Text.TypedTag[String] = {
    val index = Attribute(evt.index).textEntry
    val policy_entry = Attribute(policy).textEntry
    val took = Attribute(evt.took).textEntry
    val fg = frag(
      tr(th(index.tag), th(policy_entry.tag), th(took.tag)),
      tr(td(index.text), td(policy_entry.text), td(took.text))
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      pre(small(new SnapshotPolyglot(evt.snapshot).toYaml))
    )
  }

  private def metrics_report(evt: MetricsReport): Text.TypedTag[String] =
    metrics_event(evt, evt.serviceParams.servicePolicies.metricsReport)

  private def metrics_reset(evt: MetricsReset): Text.TypedTag[String] =
    metrics_event(evt, evt.serviceParams.servicePolicies.metricsReset)

  private def service_message(evt: ServiceMessage): Text.TypedTag[String] = {
    val domain = Attribute(evt.domain).textEntry
    val correlation = Attribute(evt.correlation).textEntry
    val alarm_level = Attribute(evt.level).textEntry

    val fg = frag(
      tr(th(domain.tag), th(correlation.tag), th(alarm_level.tag)),
      tr(td(domain.text), td(correlation.text), td(evt.level.entryName))
    )

    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      json_text(evt.message.value),
      evt.stackTrace.map(stack_trace_text)
    )
  }

  def apply[F[_]: Applicative]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(service_start)
      .withServicePanic(service_panic)
      .withServiceStop(service_stop)
      .withMetricsReport(metrics_report)
      .withMetricsReset(metrics_reset)
      .withServiceMessage(service_message)
}
