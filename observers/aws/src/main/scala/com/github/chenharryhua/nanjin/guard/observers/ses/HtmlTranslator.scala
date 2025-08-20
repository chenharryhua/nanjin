package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{Error, Event, ServiceStopCause}
import com.github.chenharryhua.nanjin.guard.translator.{htmlHelper, textConstants, textHelper, Translator}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.typelevel.cats.time.instances.all
import scalatags.Text.all.*
import scalatags.text.Builder
import scalatags.{generic, Text}

import java.time.temporal.ChronoUnit

/** https://com-lihaoyi.github.io/scalatags/
  */
private object HtmlTranslator extends all {
  import Event.*
  import htmlHelper.*
  import textConstants.*
  import textHelper.*

  private def service_table(evt: Event): generic.Frag[Builder, String] = {
    val serviceName: Text.TypedTag[String] =
      evt.serviceParams.homePage.fold(td(evt.serviceParams.serviceName.value))(hp =>
        td(a(href := hp.value)(evt.serviceParams.serviceName.value)))

    frag(
      tr(
        th(CONSTANT_TASK),
        th(CONSTANT_HOST),
        th(CONSTANT_TIMESTAMP)
      ),
      tr(
        td(evt.serviceParams.taskName.value),
        td(hostText(evt.serviceParams)),
        td(evt.timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)),
      tr(th(CONSTANT_SERVICE), th(CONSTANT_SERVICE_ID), th(CONSTANT_UPTIME)),
      tr(serviceName, td(evt.serviceParams.serviceId.show), td(uptimeText(evt)))
    )
  }

  private def json_text(js: Json): Text.TypedTag[String] =
    pre(small(js.spaces2))

  private def error_text(c: Error): Text.TypedTag[String] =
    p(b(s"$CONSTANT_CAUSE: "), pre(small(c.stack.mkString("\n\t"))))

  // events

  private def service_start(evt: ServiceStart): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_INDEX),
        th(CONSTANT_ACTIVE),
        th(CONSTANT_SNOOZED)
      ),
      tr(
        td(evt.tick.index),
        td(tookText(evt.tick.active)),
        td(tookText(evt.tick.snooze))
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      json_text(evt.serviceParams.asJson)
    )
  }

  private def service_panic(evt: ServicePanic): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_INDEX),
        th(CONSTANT_POLICY),
        th(CONSTANT_ACTIVE)
      ),
      tr(
        td(evt.tick.index),
        td(evt.serviceParams.servicePolicies.restart.policy.show),
        td(tookText(evt.tick.active))
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
    def stopCause(ssc: ServiceStopCause): Text.TypedTag[String] = ssc match {
      case ServiceStopCause.Successfully =>
        p(b(s"$CONSTANT_CAUSE: "), "Successfully")
      case ServiceStopCause.ByCancellation =>
        p(b(s"$CONSTANT_CAUSE: "), "ByCancellation")
      case ServiceStopCause.ByException(error) =>
        p(b(s"$CONSTANT_CAUSE: "), pre(small(error.stack.mkString("\n\t"))))
      case ServiceStopCause.Maintenance =>
        p(b(s"$CONSTANT_CAUSE: "), "Maintenance")
    }

    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt)),
      stopCause(evt.cause),
      json_text(evt.serviceParams.brief)
    )
  }

  private def metric_report(evt: MetricReport): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_INDEX),
        th(CONSTANT_POLICY),
        th(CONSTANT_TOOK)
      ),
      tr(
        td(metricIndexText(evt.index)),
        td(evt.serviceParams.servicePolicies.metricReport.policy.show),
        td(tookText(evt.took))
      )
    )

    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      pre(small(yamlMetrics(evt.snapshot)))
    )
  }

  private def metric_reset(evt: MetricReset): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_INDEX),
        th(CONSTANT_POLICY),
        th(CONSTANT_TOOK)
      ),
      tr(
        td(metricIndexText(evt.index)),
        td(evt.serviceParams.servicePolicies.metricReset.show),
        td(tookText(evt.took))
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      pre(small(yamlMetrics(evt.snapshot)))
    )
  }

  private def service_message(evt: ServiceMessage): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_ALARM_LEVEL),
        th(CONSTANT_MESSAGE_TOKEN)
      ),
      tr(
        td(evt.level.entryName),
        td(evt.token)
      )
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
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceMessage(service_message)
}
