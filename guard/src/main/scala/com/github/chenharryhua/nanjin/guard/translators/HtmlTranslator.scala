package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
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

  private def hostServiceText(evt: NJEvent): generic.Frag[Builder, String] = {
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

  private def actionText(evt: ActionEvent): generic.Frag[Builder, String] = {
    val tid = evt.traceId.getOrElse("none")
    frag(
      tr(td(b("Importance")), td(b("ActionID")), td(b("TraceID"))),
      tr(td(evt.actionParams.importance.show), td(evt.actionId), td(tid))
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(c.stackTrace))

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceText(evt)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief)
    )

  private def servicePanic(evt: ServicePanic): Text.TypedTag[String] = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg         = s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceText(evt)),
      p(b(msg)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Policy: "), evt.serviceParams.retryPolicy),
      causeText(evt.error)
    )
  }

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceText(evt)),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Cause: "), evt.cause.show)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(metricTitle(evt)),
      table(hostServiceText(evt)),
      pre(evt.serviceParams.brief),
      pre(evt.snapshot.show)
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(metricTitle(evt)),
      table(hostServiceText(evt)),
      pre(evt.snapshot.show)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      table(hostServiceText(evt)),
      p(b("Name: "), evt.digested.metricRepr),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceText(evt), actionText(evt)),
      p(b("Input: "), pre(evt.input.spaces2))
    )

  private def actionRetrying(evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceText(evt), actionText(evt)),
      p(b("Policy: "), evt.actionParams.retryPolicy),
      causeText(evt.error)
    )

  private def actionFailed(evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceText(evt), actionText(evt)),
      p(b("Policy: "), evt.actionInfo.actionParams.retryPolicy),
      p(b("Took: "), fmt.format(evt.took)),
      p(b("Input: "), pre(evt.input.spaces2)),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(actionTitle(evt)),
      table(hostServiceText(evt), actionText(evt)),
      p(b("Took: "), fmt.format(evt.took)),
      p(b("Output: "), pre(evt.output.spaces2))
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
