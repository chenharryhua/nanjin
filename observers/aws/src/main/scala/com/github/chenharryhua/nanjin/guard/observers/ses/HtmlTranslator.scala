package com.github.chenharryhua.nanjin.guard.observers.ses

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import com.github.chenharryhua.nanjin.guard.translator.{textConstants, textHelper, ColorScheme, Translator}
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
  import textConstants.*
  import textHelper.*

  private def coloring(evt: NJEvent): String = ColorScheme
    .decorate(evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now("color:darkgreen")
      case ColorScheme.InfoColor  => Eval.now("color:black")
      case ColorScheme.WarnColor  => Eval.now("color:#b3b300")
      case ColorScheme.ErrorColor => Eval.now("color:red")
    }
    .value

  private def host_service_table(evt: NJEvent): generic.Frag[Builder, String] = {
    val serviceName =
      evt.serviceParams.homePage.fold(td(evt.serviceParams.serviceName.value))(hp =>
        td(a(href := hp.value)(evt.serviceParams.serviceName.value)))

    frag(
      tr(
        th(CONSTANT_TIMESTAMP),
        th(CONSTANT_SERVICE_ID),
        th(CONSTANT_SERVICE),
        th(CONSTANT_TASK),
        th(CONSTANT_HOST),
        th(CONSTANT_UPTIME)
      ),
      tr(
        td(evt.timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
        td(evt.serviceParams.serviceId.show),
        serviceName,
        td(evt.serviceParams.taskName.value),
        td(evt.serviceParams.hostName.value),
        td(uptimeText(evt))
      )
    )
  }

  private def cause_text(c: NJError): Text.TypedTag[String] =
    p(b(s"$CONSTANT_CAUSE: "), pre(small(c.stack.mkString("\n"))))

  private def json_text(js: Json): Text.TypedTag[String] =
    pre(small(js.spaces2))

  // events

  private def service_started(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt)),
      json_text(evt.serviceParams.asJson)
    )

  private def service_panic(evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt)),
      p(b(panicText(evt))),
      p(b(s"$CONSTANT_POLICY: "), evt.serviceParams.servicePolicies.restart.show),
      cause_text(evt.error)
    )

  private def service_stopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt)),
      p(b(s"$CONSTANT_CAUSE: "), stopCause(evt.cause)),
      json_text(evt.serviceParams.brief)
    )

  private def metric_report(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt)),
      pre(small(yamlMetrics(evt.snapshot)))
    )

  private def metric_reset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt)),
      pre(small(yamlMetrics(evt.snapshot)))
    )

  private def service_alert(evt: ServiceAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt)),
      json_text(evt.message)
    )

  private def action_start(evt: ActionStart): Text.TypedTag[String] = {
    val start = frag(
      tr(td(b(CONSTANT_MEASUREMENT)), td(b(CONSTANT_CONFIG))),
      tr(
        td(evt.actionParams.metricName.measurement),
        td(evt.actionParams.configStr)
      )
    )
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt), start),
      json_text(evt.notes)
    )
  }

  private def action_retrying(evt: ActionRetry): Text.TypedTag[String] = {

    val retry = frag(
      tr(td(b(CONSTANT_MEASUREMENT)), td(b(CONSTANT_CONFIG))),
      tr(
        td(evt.actionParams.metricName.measurement),
        td(evt.actionParams.configStr)
      )
    )
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt), retry),
      p(b(retryText(evt))),
      p(b(s"$CONSTANT_POLICY: "), evt.actionParams.retryPolicy.show),
      cause_text(evt.error)
    )
  }

  private def action_result_table(evt: ActionResultEvent): generic.Frag[Builder, String] =
    frag(
      tr(
        td(b(CONSTANT_MEASUREMENT)),
        td(b(CONSTANT_CONFIG)),
        td(b(CONSTANT_TOOK))
      ),
      tr(
        td(evt.actionParams.metricName.measurement),
        td(evt.actionParams.configStr),
        td(evt match {
          case af: ActionFail => tookText(af.took)
          case ad: ActionDone => tookText(ad.took)
        })
      )
    )

  private def action_fail(evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt), action_result_table(evt)),
      p(b(s"$CONSTANT_POLICY: "), evt.actionParams.retryPolicy.show),
      cause_text(evt.error),
      json_text(evt.notes)
    )

  private def action_done(evt: ActionDone): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(host_service_table(evt), action_result_table(evt)),
      json_text(evt.notes)
    )

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
