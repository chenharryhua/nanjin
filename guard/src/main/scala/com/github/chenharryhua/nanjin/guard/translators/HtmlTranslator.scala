package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import org.typelevel.cats.time.instances.all
import scalatags.{generic, Text}
import scalatags.Text.all.*
import scalatags.text.Builder

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
      evt.serviceParams.taskParams.homePage.fold(td(evt.serviceName.value))(hp =>
        td(a(href := hp.value)(evt.serviceName.value)))

    frag(
      tr(
        th("Timestamp"),
        th("Service"),
        th("Task"),
        th("Host"),
        th("ServiceID")
      ),
      tr(
        td(evt.timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
        serviceName,
        td(evt.serviceParams.taskParams.taskName.value),
        td(evt.serviceParams.taskParams.hostName.value),
        td(evt.serviceId.show)
      )
    )
  }

  private def actionTable(evt: ActionEvent): generic.Frag[Builder, String] = {
    val tid = evt.traceId.getOrElse("none")
    frag(
      tr(td(b("Importance")), td(b("ActionID")), td(b("TraceID"))),
      tr(td(evt.actionParams.importance.show), td(evt.actionId), td(tid))
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(small(c.stackTrace)))
  private def snapshotText(ss: MetricSnapshot): Text.TypedTag[String] = pre(small(ss.show))
  private def jsonText(js: Json): Text.TypedTag[String]               = pre(small(js.spaces2))
  private def briefText(brief: String): Text.TypedTag[String]         = pre(small(brief))

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      briefText(evt.serviceParams.brief)
    )

  private def servicePanic(evt: ServicePanic): Text.TypedTag[String] = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg         = s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      p(b(msg)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Policy: "), evt.serviceParams.retryPolicy),
      causeText(evt.error)
    )
  }

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceTable(evt)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Cause: "), evt.cause.show)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(metricTitle(evt)),
      table(hostServiceTable(evt)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      snapshotText(evt.snapshot)
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(metricTitle(evt)),
      table(hostServiceTable(evt)),
      briefText(evt.serviceParams.brief),
      snapshotText(evt.snapshot)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(instantEventTitle(evt)),
      table(hostServiceTable(evt)),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), actionTable(evt)),
      p(b("Input: "), jsonText(evt.input))
    )

  private def actionRetrying(evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), actionTable(evt)),
      p(b("Policy: "), evt.actionParams.retryPolicy),
      causeText(evt.error)
    )

  private def actionFailed(evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), actionTable(evt)),
      p(b("Policy: "), evt.actionInfo.actionParams.retryPolicy),
      p(b("Took: "), fmt.format(evt.took)),
      p(b("Input: "), jsonText(evt.input)),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceTable(evt), actionTable(evt)),
      p(b("Took: "), fmt.format(evt.took)),
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
      .withActionSucc(actionSucced)
}
