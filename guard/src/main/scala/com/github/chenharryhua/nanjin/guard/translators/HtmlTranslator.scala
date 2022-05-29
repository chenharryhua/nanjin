package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval, Monad}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{ActionInfo, NJError, NJEvent}
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

  private def timestampText(timestamp: ZonedDateTime): Text.TypedTag[String] =
    p(b("Timestamp: "), timestamp.truncatedTo(ChronoUnit.SECONDS).show)

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("Number of retries: "), numRetry.toString)

  private def hostServiceText(si: ServiceParams): Text.TypedTag[String] = {
    val sn = si.taskParams.homePage.fold(p(b("Service: "), si.serviceName.value))(hp =>
      p(b("Sevice: "), a(href := hp.value)(si.serviceName.value)))
    div(
      sn,
      p(b("Host: "), si.taskParams.hostName.value)
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(c.stackTrace))

  private def pendingActions(oas: List[ActionInfo], now: ZonedDateTime): Text.TypedTag[String] = {
    val tds = "border: 1px solid #dddddd; text-align: left; padding: 8px;"
    div(
      b("Ongoing actions:"),
      table(style := "font-family: arial, sans-serif; border-collapse: collapse; width: 100%;")(
        tr(
          th(style := tds)("name"),
          th(style := tds)("digest"),
          th(style := tds)("so far took"),
          th(style := tds)("launch time"),
          th(style := tds)("id")),
        oas.map(a =>
          tr(
            td(style := tds)(a.actionParams.digested.origin),
            td(style := tds)(a.actionParams.digested.digest),
            td(style := tds)(fmt.format(a.launchTime, now)),
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
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief)
    )

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b(upcomingRestartTimeInterpretation(evt))),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Policy: "), evt.serviceParams.retry.policy[F].show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      causeText(evt.error)
    )

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Cause: "), evt.cause.show)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(upcomingRestartTimeInterpretation(evt)),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      pre(evt.serviceParams.brief),
      pendingActions(evt.ongoings, evt.timestamp),
      pre(evt.snapshot.show)
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.snapshot.show)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Name: "), evt.metricName.metricRepr),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Input: "), pre(evt.info.spaces2))
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Policy: "), evt.actionParams.retry.policy[F].show),
      causeText(evt.error)
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Policy: "), evt.actionInfo.actionParams.retry.policy[F].show),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      p(b("Input: "), pre(evt.info.spaces2)),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.title),
      p(b("Name: "), evt.digested.metricRepr),
      p(b("ID: "), evt.actionID.show),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      p(b("Output: "), pre(evt.info.spaces2))
    )

  def apply[F[_]: Monad]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic[F])
      .withServiceStop(serviceStopped)
      .withMetricsReport(metricReport)
      .withMetricsReset(metricReset)
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying[F])
      .withActionFail(actionFailed[F])
      .withActionSucc(actionSucced)
}
