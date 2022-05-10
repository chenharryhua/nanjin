package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

/** https://com-lihaoyi.github.io/scalatags/
  */
private object HtmlTranslator extends all {

  private val coloring: Coloring = new Coloring({
    case ColorScheme.GoodColor  => "color:black"
    case ColorScheme.InfoColor  => "color:black"
    case ColorScheme.WarnColor  => "color:#FF9333"
    case ColorScheme.ErrorColor => "color:red"
  })

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

  private def notesText(n: Notes): Text.TypedTag[String]   = pre(n.value)
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
            td(style := tds)(a.actionParams.metricName.origin),
            td(style := tds)(a.actionParams.metricName.digest),
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
      h3(style := coloring(evt))("Service Started"),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief)
    )

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))("Service Panic"),
      p(b(upcomingRestartTimeInterpretation(evt))),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("ErrorID: "), evt.error.uuid.show),
      p(b("Policy: "), evt.serviceParams.retry.policy[F].show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      causeText(evt.error)
    )

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))("Service Stopped"),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      p(b("Cause: "), evt.cause.show)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.reportType.show),
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
      h3(style := coloring(evt))(evt.resetType.show),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("UpTime: "), fmt.format(evt.upTime)),
      pre(evt.snapshot.show)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))("Service Alert"),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("Name: "), evt.metricName.metricRepr),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.actionParams.startTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("ActionID: "), evt.actionID.show)
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.actionParams.retryTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("ActionID: "), evt.actionID.show),
      p(b("Policy: "), evt.actionParams.retry.policy[F].show),
      causeText(evt.error)
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.actionParams.failedTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("ActionID: "), evt.actionID.show),
      p(b("Policy: "), evt.actionInfo.actionParams.retry.policy[F].show),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      notesText(evt.notes),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(evt.actionParams.succedTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("ServiceID: "), evt.serviceID.show),
      p(b("ActionID: "), evt.actionID.show),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      notesText(evt.notes)
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
