package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval, Monad}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.event.{NJError, NJEvent}
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

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

  private def hostServiceText(evt: NJEvent): Text.TypedTag[String] = {
    val serviceName =
      evt.serviceParams.taskParams.homePage.fold(p(b("Service: "), evt.serviceName.value))(hp =>
        p(b("Sevice: "), a(href := hp.value)(evt.serviceName.value)))
    div(
      p(b("Timestamp: "), evt.timestamp.toLocalDateTime.truncatedTo(ChronoUnit.SECONDS).show),
      p(b("Host: "), evt.serviceParams.taskParams.hostName.value),
      p(b("Task: "), evt.serviceParams.taskParams.taskName.value),
      serviceName,
      p(b("ServiceID: "), evt.serviceId.show)
    )
  }

  private def actionText(evt: ActionEvent): Text.TypedTag[String] = {
    val tid = evt.traceId.getOrElse("none")
    div(
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionId.show),
      p(b("Trace ID: "), tid)
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(c.stackTrace))

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      hostServiceText(evt),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief)
    )

  private def servicePanic(evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b(upcomingRestartTimeInterpretation(evt))),
      hostServiceText(evt),
      p(b("Policy: "), evt.serviceParams.retryPolicy),
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
      actionText(evt),
      hostServiceText(evt),
      p(b("Input: "), pre(evt.input.spaces2))
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      actionText(evt),
      hostServiceText(evt),
      p(b("Policy: "), evt.actionParams.retry.policy[F].show),
      causeText(evt.error)
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      actionText(evt),
      hostServiceText(evt),
      p(b("Policy: "), evt.actionInfo.actionParams.retry.policy[F].show),
      p(b("Took: "), fmt.format(evt.took)),
      p(b("Input: "), pre(evt.input.spaces2)),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      actionText(evt),
      hostServiceText(evt),
      p(b("Took: "), fmt.format(evt.took)),
      p(b("Output: "), pre(evt.output.spaces2))
    )

  def apply[F[_]: Monad]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic)
      .withServiceStop(serviceStopped)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying[F])
      .withActionFail(actionFailed[F])
      .withActionSucc(actionSucced)
}
