package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import org.typelevel.cats.time.instances.all
import scalatags.Text.all.*
import scalatags.text.Builder
import scalatags.{generic, Text}

import java.time.temporal.ChronoUnit

/** https://com-lihaoyi.github.io/scalatags/
  */
private object HtmlTranslator extends all {
  import NJEvent.*

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
      evt.serviceParams.homePage.fold(td(evt.serviceName.value))(hp =>
        td(a(href := hp.value)(evt.serviceName.value)))

    frag(
      tr(
        th("Timestamp"),
        th("Service"),
        th("Task"),
        th("Host"),
        th("ServiceID"),
        th("UpTime")
      ),
      tr(
        td(evt.timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
        serviceName,
        td(evt.serviceParams.taskParams.taskName.value),
        td(evt.serviceParams.taskParams.hostName.value),
        td(evt.serviceId.show),
        td(fmt.format(evt.upTime))
      )
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(small(c.stackTrace)))
  private def snapshotText(sp: ServiceParams, ss: MetricSnapshot): Text.TypedTag[String] =
    pre(small(showSnapshot(sp, ss)))
  private def jsonText(js: Json): Text.TypedTag[String]     = pre(small(js.spaces2))
  private def briefText(brief: Json): Text.TypedTag[String] = pre(small(brief.spaces2))

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      briefText(evt.serviceParams.brief)
    )

  private def servicePanic(evt: ServicePanic): Text.TypedTag[String] = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg         = s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      p(b(msg)),
      p(b("Policy: "), evt.serviceParams.retryPolicy),
      causeText(evt.error)
    )
  }

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      p(b("Cause: "), evt.cause.show),
      briefText(evt.serviceParams.brief)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      snapshotText(evt.serviceParams, evt.snapshot)
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      briefText(evt.serviceParams.brief),
      snapshotText(evt.serviceParams, evt.snapshot)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(instantEventTitle(evt)),
      table(hostServiceTable(evt)),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] = {
    val start = frag(
      tr(td(b("Importance")), td(b("ActionID")), td(b("TraceID"))),
      tr(td(evt.actionParams.importance.show), td(evt.actionId), td(evt.traceId))
    )
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), start),
      p(b("Input: "), jsonText(evt.input))
    )
  }

  private def actionRetrying(evt: ActionRetry): Text.TypedTag[String] = {

    val retry = frag(
      tr(td(b("Importance")), td(b("ActionID")), td(b("TraceID")), td(b("Index")), td(b("Resume"))),
      tr(
        td(evt.actionParams.importance.show),
        td(evt.actionId),
        td(evt.traceId),
        td(evt.retriesSoFar + 1),
        td(evt.resumeTime.toLocalTime.show))
    )
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), retry),
      p(b("Policy: "), evt.actionParams.retryPolicy),
      causeText(evt.error)
    )
  }

  private def actionResultTable(evt: ActionResultEvent): generic.Frag[Builder, String] =
    frag(
      tr(td(b("Importance")), td(b("ActionID")), td(b("TraceID")), td(b("Took"))),
      tr(td(evt.actionParams.importance.show), td(evt.actionId), td(evt.traceId), td(fmt.format(evt.took)))
    )

  private def actionFailed(evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), actionResultTable(evt)),
      p(b("Policy: "), evt.actionInfo.actionParams.retryPolicy),
      p(b("Input: "), jsonText(evt.input)),
      causeText(evt.error)
    )

  private def actionCompleted(evt: ActionComplete): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), actionResultTable(evt)),
      p(b("Output: "), jsonText(evt.output))
    )

  def apply[F[_]: Applicative]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic)
      .withServiceStop(serviceStopped)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionComplete(actionCompleted)
}
