package com.github.chenharryhua.nanjin.guard.translators

import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZonedDateTime

/** https://com-lihaoyi.github.io/scalatags/
  */
private[translators] object HtmlTranslator extends all {
  private def timestampText(timestamp: ZonedDateTime): Text.TypedTag[String] =
    p(b("timestamp: "), localTimestampStr(timestamp))

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("number of retries: "), numRetry.toString)

  private def hostServiceText(si: ServiceParams): Text.TypedTag[String] =
    p(b("service: "), si.metricName.metricRepr, "    ", b("host: "), si.taskParams.hostName)

  private def notesText(n: Notes): Text.TypedTag[String]      = p(b("notes: "), pre(n.value))
  private def causeText(c: NJError): Text.TypedTag[String]    = p(b("cause: "), pre(c.stackTrace))
  private def brief(si: ServiceParams): Text.TypedTag[String] = p(b("brief: "), pre(si.brief))

  private def pendingActions(oas: List[OngoingAction], now: ZonedDateTime): Text.TypedTag[String] = {
    val tds = "border: 1px solid #dddddd; text-align: left; padding: 8px;"
    div(
      b("ongoing actions:"),
      table(style := "font-family: arial, sans-serif; border-collapse: collapse; width: 100%;")(
        tr(
          th(style := tds)("name"),
          th(style := tds)("so far took"),
          th(style := tds)("launch time"),
          th(style := tds)("id")),
        oas.map(a =>
          tr(
            td(style := tds)(a.metricName.metricRepr),
            td(style := tds)(fmt.format(a.launchTime, now)),
            td(style := tds)(localTimestampStr(a.launchTime)),
            td(style := tds)(a.uuid.show)
          ))
      )
    )
  }

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(h3(s"Service Started"), timestampText(evt.timestamp), hostServiceText(evt.serviceParams))

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(s"Service Panic"),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("restart so far: "), evt.retryDetails.retriesSoFar),
      p(b("error ID: "), evt.error.uuid.show),
      p(b("policy: "), evt.serviceParams.retry.policy[F].show),
      brief(evt.serviceParams),
      causeText(evt.error)
    )

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := "color:blue")(s"Service Stopped"),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      pre(evt.snapshot.show)
    )

  private def metricsReport(evt: MetricsReport): Text.TypedTag[String] = {
    val color: String = if (evt.hasError) "color:red" else "color:black"
    div(
      h3(style := color)(evt.reportType.show),
      p(serviceStatusWord(evt.serviceStatus)),
      timestampText(evt.timestamp),
      p(b("Time Zone: "), evt.serviceParams.taskParams.zoneId.show),
      hostServiceText(evt.serviceParams),
      p(b("up time: "), fmt.format(evt.upTime)),
      pendingActions(evt.ongoings, evt.timestamp),
      brief(evt.serviceParams),
      pre(evt.snapshot.show)
    )
  }

  private def metricsReset(evt: MetricsReset): Text.TypedTag[String] = {
    val color: String = if (evt.hasError) "color:red" else "color:black"
    div(
      h3(style := color)(evt.resetType.show),
      p(serviceStatusWord(evt.serviceStatus)),
      timestampText(evt.timestamp),
      p(b("Time Zone: "), evt.serviceParams.taskParams.zoneId.show),
      hostServiceText(evt.serviceParams),
      brief(evt.serviceParams),
      pre(evt.snapshot.show)
    )
  }

  private def serviceAlert(evt: ServiceAlert): Text.TypedTag[String] =
    div(
      h3("Service Alert"),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b("name: "), evt.metricName.metricRepr, "    ", b("importance: "), evt.importance.show),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(evt.actionParams.startTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionInfo.actionParams.alias} ID: "), evt.actionInfo.uuid.show)
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(evt.actionParams.retryTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionInfo.actionParams.alias} ID: "), evt.actionInfo.uuid.show),
      p(b("policy: "), evt.actionInfo.actionParams.retry.policy[F].show)
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(evt.actionParams.failedTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionParams.alias} ID: "), evt.actionInfo.uuid.show),
      p(b("error ID: "), evt.error.uuid.show),
      p(b("policy: "), evt.actionInfo.actionParams.retry.policy[F].show),
      p(b("took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      notesText(evt.notes),
      brief(evt.serviceParams),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(evt.actionParams.succedTitle),
      timestampText(evt.timestamp),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionParams.alias} ID: "), evt.actionInfo.uuid.show),
      p(b("took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      notesText(evt.notes)
    )

  def apply[F[_]: Monad]: Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic[F])
      .withServiceStop(serviceStopped)
      .withMetricsReport(metricsReport)
      .withMetricsReset(metricsReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying[F])
      .withActionFail(actionFailed[F])
      .withActionSucc(actionSucced)
}
