package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.Applicative
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.config.Attribute
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, Index, Took}
import com.github.chenharryhua.nanjin.guard.translator.{htmlHelper, textConstants, textHelper, Translator}
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
  import textConstants.*
  import textHelper.*

  private def service_table(evt: Event): generic.Frag[Builder, String] = {
    val task_name = Attribute(evt.serviceParams.taskName).textEntry
    val host = Attribute(evt.serviceParams.host).textEntry
    val service_name = Attribute(evt.serviceParams.serviceName).textEntry
    val homepage =
      evt.serviceParams.homepage.fold(td(service_name.text))(hp => td(a(href := hp.value)(service_name.text)))
    val service_id = Attribute(evt.serviceParams.serviceId).textEntry
    val uptime = Attribute(evt.upTime).textEntry
    val timestamp = Attribute(evt.timestamp).textEntry

    frag(
      tr(th(task_name.tag), th(host.tag), th(timestamp.tag)),
      tr(td(task_name.text), td(host.text), td(timestamp.text)),
      tr(th(service_name.tag), th(service_id.tag), th(uptime.tag)),
      tr(homepage, td(service_id.text), td(uptime.text))
    )
  }

  private def json_text(js: Json): Text.TypedTag[String] =
    pre(small(js.spaces2))

  private def error_text(c: Error): Text.TypedTag[String] = {
    val err = Attribute(c).textEntry
    p(b(s"${err.tag}: "), pre(small(err.text)))
  }

  // events

  private def service_start(evt: ServiceStart): Text.TypedTag[String] = {
    val index = Attribute(Index(evt.tick.index)).textEntry

    val fg = frag(
      tr(
        th(index.tag),
        th(CONSTANT_ACTIVE),
        th(CONSTANT_SNOOZED)
      ),
      tr(
        td(index.text),
        td(Took(evt.tick.active).show),
        td(Took(evt.tick.snooze).show)
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      json_text(evt.serviceParams.simpleJson)
    )
  }

  private def service_panic(evt: ServicePanic): Text.TypedTag[String] = {
    val policy = Attribute(evt.serviceParams.servicePolicies.restart.policy).textEntry
    val index = Attribute(Index(evt.tick.index)).textEntry

    val fg = frag(
      tr(
        th(index.tag),
        th(policy.tag),
        th(CONSTANT_ACTIVE)
      ),
      tr(
        td(index.text),
        td(policy.text),
        td(Took(evt.tick.active).show)
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      p(b(panicText(evt))),
      error_text(evt.error)
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

  private def metrics_event(evt: MetricsEvent): Text.TypedTag[String] = {
    val index = Attribute(evt.index).textEntry
    val policy = Attribute(evt.serviceParams.servicePolicies.metricsReport).textEntry
    val took = Attribute(evt.took).textEntry
    val fg = frag(
      tr(th(index.tag), th(policy.tag), th(took.tag)),
      tr(td(index.text), td(policy.text), td(took.text))
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      pre(small(yamlMetrics(evt.snapshot)))
    )
  }

  private def metrics_report(evt: MetricsReport): Text.TypedTag[String] =
    metrics_event(evt)

  private def metrics_reset(evt: MetricsReset): Text.TypedTag[String] =
    metrics_event(evt)

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
      json_text(evt.message),
      evt.error.map(error_text)
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
