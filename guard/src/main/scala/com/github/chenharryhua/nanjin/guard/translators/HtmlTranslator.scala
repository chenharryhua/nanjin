package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
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

  private def hostServiceTable(evt: NJEvent): generic.Frag[Builder, String] = {
    val serviceName =
      evt.serviceParams.taskParams.homePage.fold(td(evt.serviceParams.serviceName))(hp =>
        td(a(href := hp)(evt.serviceParams.serviceName)))

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
        td(evt.serviceParams.taskParams.taskName),
        td(evt.serviceParams.taskParams.hostName.value),
        td(upTimeText(evt))
      )
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] =
    p(b(s"$CONSTANT_CAUSE: "), pre(small(c.stackTrace)))

  private def jsonText(js: Json): Text.TypedTag[String] =
    pre(small(js.spaces2))

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      jsonText(evt.serviceParams.asJson)
    )

  private def servicePanic(evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      p(b(panicText(evt).replace("*", ""))),
      p(b(s"$CONSTANT_POLICY: "), evt.serviceParams.restartPolicy),
      causeText(evt.error)
    )

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      p(b(s"$CONSTANT_CAUSE: "), evt.cause.show),
      evt.serviceParams.brief.fold(div())(jsonText)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      pre(small(yamlMetrics(evt.snapshot, evt.serviceParams.metricParams)))
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      pre(small(yamlMetrics(evt.snapshot, evt.serviceParams.metricParams)))
    )

  private def serviceAlert(evt: ServiceAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      jsonText(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] = {
    val start = frag(
      tr(
        td(b(CONSTANT_ACTION_ID)),
        td(b(CONSTANT_MEASUREMENT)),
        td(b(CONSTANT_IMPORTANCE)),
        td(b(CONSTANT_STRATEGY))),
      tr(
        td(evt.actionId),
        td(evt.actionParams.metricId.metricName.measurement),
        td(evt.actionParams.importance.entryName),
        td(evt.actionParams.publishStrategy.entryName)
      )
    )
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), start),
      evt.notes.fold(div())(js => jsonText(js))
    )
  }

  private def actionRetrying(evt: ActionRetry): Text.TypedTag[String] = {

    val retry = frag(
      tr(
        td(b(CONSTANT_ACTION_ID)),
        td(b(CONSTANT_MEASUREMENT)),
        td(b(CONSTANT_IMPORTANCE)),
        td(b(CONSTANT_STRATEGY))),
      tr(
        td(evt.actionId),
        td(evt.actionParams.metricId.metricName.measurement),
        td(evt.actionParams.importance.entryName),
        td(evt.actionParams.publishStrategy.entryName)
      )
    )
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), retry),
      p(b(retryText(evt).replace("*", ""))),
      p(b(s"$CONSTANT_POLICY: "), evt.actionParams.retryPolicy),
      causeText(evt.error)
    )
  }

  private def actionResultTable(evt: ActionResultEvent): generic.Frag[Builder, String] =
    frag(
      tr(
        td(b(CONSTANT_ACTION_ID)),
        td(b(CONSTANT_MEASUREMENT)),
        td(b(CONSTANT_IMPORTANCE)),
        td(b(CONSTANT_STRATEGY)),
        td(b(CONSTANT_TOOK))
      ),
      tr(
        td(evt.actionId),
        td(evt.actionParams.metricId.metricName.measurement),
        td(evt.actionParams.importance.entryName),
        td(evt.actionParams.publishStrategy.entryName),
        td(tookText(evt.took))
      )
    )

  private def actionFailed(evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), actionResultTable(evt)),
      p(b(s"$CONSTANT_POLICY: "), evt.actionParams.retryPolicy),
      causeText(evt.error),
      evt.notes.fold(div())(js => jsonText(js))
    )

  private def actionDone(evt: ActionDone): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), actionResultTable(evt)),
      evt.notes.fold(div())(js => jsonText(js))
    )

  def apply[F[_]: Applicative]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic)
      .withServiceStop(serviceStopped)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionDone(actionDone)
}
