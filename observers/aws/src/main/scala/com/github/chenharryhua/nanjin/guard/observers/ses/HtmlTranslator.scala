package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
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
  import NJEvent.*
  import htmlHelper.*
  import textConstants.*
  import textHelper.*

  private def service_table(evt: NJEvent): generic.Frag[Builder, String] = {
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

  private def cause_text(c: NJError): Text.TypedTag[String] =
    p(b(s"$CONSTANT_CAUSE: "), pre(small(c.stack.mkString("\n\t"))))

  private def json_text(js: Json): Text.TypedTag[String] =
    pre(small(js.spaces2))

  // events

  private def service_started(evt: ServiceStart): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_INDEX),
        th(CONSTANT_POLICY),
        th(CONSTANT_TIMEZONE)
      ),
      tr(
        td(evt.tick.index),
        td(evt.serviceParams.servicePolicies.restart.show),
        td(evt.serviceParams.zoneId.show)
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      json_text(evt.serviceParams.asJson)
    )
  }

  private def service_panic(evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt)),
      p(b(panicText(evt))),
      p(b(s"$CONSTANT_POLICY: "), evt.serviceParams.servicePolicies.restart.show),
      cause_text(evt.error)
    )

  private def service_stopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt)),
      p(b(s"$CONSTANT_CAUSE: "), stopCause(evt.cause)),
      json_text(evt.serviceParams.brief)
    )

  private def metric_report(evt: MetricReport): Text.TypedTag[String] = {
    val fg = frag(
      tr(
        th(CONSTANT_INDEX),
        th(CONSTANT_POLICY),
        th(CONSTANT_TOOK)
      ),
      tr(
        td(metricIndexText(evt.index)),
        td(evt.serviceParams.servicePolicies.metricReport.show),
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

  private def service_alert(evt: ServiceAlert): Text.TypedTag[String] = {
    val fg = frag(
      tr(th(CONSTANT_MEASUREMENT), th(CONSTANT_ALERT_ID), th(CONSTANT_NAME)),
      tr(
        td(evt.metricName.measurement),
        td(s"${evt.alertID.show}/${evt.metricName.digest}"),
        td(evt.metricName.name)
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), fg),
      json_text(evt.message)
    )
  }

  private def action_section(evt: ActionEvent): generic.Frag[Builder, String] =
    frag(
      tr(th(CONSTANT_MEASUREMENT), th(CONSTANT_ACTION_ID), th(CONSTANT_NAME)),
      tr(
        td(evt.actionParams.metricName.measurement),
        td(s"${evt.actionID.show}/${evt.actionParams.metricName.digest}"),
        td(evt.actionParams.metricName.name)
      )
    )

  private def action_start(evt: ActionStart): Text.TypedTag[String] = {
    val fg = frag(
      tr(th(CONSTANT_CONFIG), th(CONSTANT_POLICY)),
      tr(
        td(evt.actionParams.configStr),
        td(evt.actionParams.retryPolicy.show)
      ))
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), action_section(evt), fg),
      json_text(evt.notes)
    )
  }

  private def action_retrying(evt: ActionRetry): Text.TypedTag[String] = {
    val fg = frag(
      tr(th(CONSTANT_CONFIG), th(CONSTANT_POLICY)),
      tr(
        td(evt.actionParams.configStr),
        td(evt.actionParams.retryPolicy.show)
      ))
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), action_section(evt), fg),
      p(b(retryText(evt))),
      p(b(s"$CONSTANT_CAUSE: "), small(evt.error.message)),
      json_text(evt.notes)
    )
  }

  private def action_fail(evt: ActionFail): Text.TypedTag[String] = {
    val fg = frag(
      tr(th(CONSTANT_CONFIG), th(CONSTANT_POLICY)),
      tr(
        td(evt.actionParams.configStr),
        td(evt.actionParams.retryPolicy.show)
      ))
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), action_section(evt), fg),
      cause_text(evt.error),
      json_text(evt.notes)
    )
  }

  private def action_done(evt: ActionDone): Text.TypedTag[String] = {
    val fg = frag(
      tr(th(CONSTANT_CONFIG), th(CONSTANT_POLICY), th(CONSTANT_TOOK)),
      tr(
        td(evt.actionParams.configStr),
        td(evt.actionParams.retryPolicy.show),
        td(tookText(evt.took))
      )
    )
    div(
      h3(style := htmlColoring(evt))(eventTitle(evt)),
      table(service_table(evt), action_section(evt), fg),
      json_text(evt.notes)
    )
  }

  def apply[F[_]: Applicative]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(service_started)
      .withServicePanic(service_panic)
      .withServiceStop(service_stopped)
      .withMetricReport(metric_report)
      .withMetricReset(metric_reset)
      .withServiceAlert(service_alert)
      .withActionStart(action_start)
      .withActionRetry(action_retrying)
      .withActionFail(action_fail)
      .withActionDone(action_done)
}
