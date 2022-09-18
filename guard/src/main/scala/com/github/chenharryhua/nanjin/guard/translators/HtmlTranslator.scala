package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval, Monad}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent, OngoingAction}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZonedDateTime
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

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("Number of retries: "), numRetry.toString)

  private def hostServiceText(evt: NJEvent): Text.TypedTag[String] = {
    val serviceName =
      evt.serviceParams.taskParams.homePage.fold(p(b("Service: "), evt.serviceName.value))(hp =>
        p(b("Sevice: "), a(href := hp.value)(evt.serviceName.value)))
    div(
      p(b("Timestamp: "), evt.timestamp.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show),
      p(b("Host: "), evt.serviceParams.taskParams.hostName.value),
      p(b("Task: "), evt.serviceParams.taskParams.taskName.value),
      serviceName,
      p(b("ServiceID: "), evt.serviceID.show)
    )
  }

  private def trace(traceID: String, traceUri: Option[String]): Text.TypedTag[String] =
    traceUri.map(uri => p(b("Trace ID: "), a(href := uri)(traceID))).getOrElse(p(b("Trace ID: "), traceID))

  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(c.stackTrace))

  private def pendingActions(oas: List[OngoingAction], now: ZonedDateTime): Text.TypedTag[String] = {
    val tds = "border: 1px solid #dddddd; text-align: left; padding: 8px;"
    div(
      b("Ongoing actions:"),
      table(style := "font-family: arial, sans-serif; border-collapse: collapse; width: 100%;")(
        tr(
          th(style := tds)("Name"),
          th(style := tds)("Digest"),
          th(style := tds)("Up Time"),
          th(style := tds)("Launch Time"),
          th(style := tds)("ID")),
        oas.map(a =>
          tr(
            td(style := tds)(a.digested.name),
            td(style := tds)(a.digested.digest),
            td(style := tds)(fmt.format(a.took(now))),
            td(style := tds)(a.launchTime.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show),
            td(style := tds)(a.actionID.show)
          ))
      )
    )
  }

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      hostServiceText(evt),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief)
    )

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b(upcomingRestartTimeInterpretation(evt))),
      hostServiceText(evt),
      p(b("Policy: "), evt.serviceParams.retry.policy[F].show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      causeText(evt.error)
    )

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      hostServiceText(evt),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Cause: "), evt.cause.show)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(upcomingRestartTimeInterpretation(evt)),
      hostServiceText(evt),
      pre(evt.serviceParams.brief),
      pendingActions(evt.ongoings, evt.timestamp),
      pre(evt.snapshot.show)
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      hostServiceText(evt),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.snapshot.show)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      hostServiceText(evt),
      p(b("Name: "), evt.digested.metricRepr),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      trace(evt.traceID, evt.actionInfo.traceUri),
      hostServiceText(evt),
      p(b("Input: "), pre(evt.input.spaces2))
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      trace(evt.traceID, evt.actionInfo.traceUri),
      hostServiceText(evt),
      p(b("Policy: "), evt.actionParams.retry.policy[F].show),
      causeText(evt.error)
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      trace(evt.traceID, evt.actionInfo.traceUri),
      hostServiceText(evt),
      p(b("Policy: "), evt.actionInfo.actionParams.retry.policy[F].show),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      p(b("Input: "), pre(evt.input.spaces2)),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      trace(evt.traceID, evt.actionInfo.traceUri),
      hostServiceText(evt),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      p(b("Output: "), pre(evt.output.spaces2))
    )

  def apply[F[_]: Monad]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic[F])
      .withServiceStop(serviceStopped)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying[F])
      .withActionFail(actionFailed[F])
      .withActionSucc(actionSucced)
}
