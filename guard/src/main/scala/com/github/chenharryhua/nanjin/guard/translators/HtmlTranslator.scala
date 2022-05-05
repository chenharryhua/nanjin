package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.{Instant, ZoneId, ZonedDateTime}

/** https://com-lihaoyi.github.io/scalatags/
  */
private[translators] object HtmlTranslator extends all {
  private def timestampText(timestamp: ZonedDateTime): Text.TypedTag[String] =
    p(b("Timestamp: "), localTimestampStr(timestamp))

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("Number of retries: "), numRetry.toString)

  private def hostServiceText(si: ServiceParams): Text.TypedTag[String] = {
    val sn = si.taskParams.homePage.fold(p(b("Service: "), si.metricName.metricRepr))(hp =>
      p(b("Sevice: "), a(href := hp.value)(si.metricName.origin)))
    div(
      sn,
      p(b("Host: "), si.taskParams.hostName.value)
    )
  }

  private def notesText(n: Notes): Text.TypedTag[String]   = pre(n.value)
  private def causeText(c: NJError): Text.TypedTag[String] = p(b("cause: "), pre(c.stackTrace))

  private def pendingActions(oas: List[OngoingAction], now: Instant, zoneId: ZoneId): Text.TypedTag[String] = {
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
            td(style := tds)(a.metricName.origin),
            td(style := tds)(a.metricName.digest),
            td(style := tds)(fmt.format(a.launchTime, now)),
            td(style := tds)(localTimestampStr(a.launchTime.atZone(zoneId))),
            td(style := tds)(a.uniqueId.show)
          ))
      )
    )
  }

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(s"Service Started"),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b("Service ID: "), evt.uuid.show),
      p(b("Up Time: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief)
    )

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(s"Service Panic"),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b("Restart so far: "), evt.retryDetails.retriesSoFar),
      p(b("Error ID: "), evt.error.uuid.show),
      p(b("Policy: "), evt.serviceParams.retry.policy[F].show),
      p(b("Service ID: "), evt.uuid.show),
      p(b("Up Time: "), fmt.format(evt.upTime)),
      causeText(evt.error)
    )

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(s"Service Stopped"),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b("Service ID: "), evt.uuid.show),
      p(b("Up Time: "), fmt.format(evt.upTime)),
      p(b("cause: "), evt.cause.show)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] = {
    val color: String = if (evt.hasError) "color:red" else "color:black"
    div(
      h3(style := color)(evt.reportType.show),
      p(serviceStatusWord(evt.serviceStatus, evt.zoneId)),
      p(b("Service ID: "), evt.uuid.show),
      timestampText(evt.zonedDateTime),
      p(b("Time Zone: "), evt.serviceParams.taskParams.zoneId.show),
      hostServiceText(evt.serviceParams),
      p(b("Service ID: "), evt.uuid.show),
      p(b("Up Time: "), fmt.format(evt.upTime)),
      pre(evt.serviceParams.brief),
      pendingActions(evt.ongoings, evt.timestamp, evt.zoneId),
      pre(evt.snapshot.show)
    )
  }

  private def metricReset(evt: MetricReset): Text.TypedTag[String] = {
    val color: String = if (evt.hasError) "color:red" else "color:black"
    div(
      h3(style := color)(evt.resetType.show),
      p(serviceStatusWord(evt.serviceStatus, evt.zoneId)),
      timestampText(evt.zonedDateTime),
      p(b("Time Zone: "), evt.serviceParams.taskParams.zoneId.show),
      hostServiceText(evt.serviceParams),
      p(b("Service ID: "), evt.uuid.show),
      p(b("Up Time: "), fmt.format(evt.upTime)),
      pre(evt.snapshot.show)
    )
  }

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3("Service Alert"),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b("Name: "), evt.metricName.metricRepr, "    ", b("Importance: "), evt.importance.show),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] =
    div(
      h3(evt.actionParams.startTitle),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionParams.catalog} ID: "), evt.actionInfo.uniqueId.show)
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): Text.TypedTag[String] =
    div(
      h3(evt.actionParams.retryTitle),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionParams.catalog} ID: "), evt.actionInfo.uniqueId.show),
      p(b("Policy: "), evt.actionParams.retry.policy[F].show)
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(evt.actionParams.failedTitle),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionParams.catalog} ID: "), evt.actionInfo.uniqueId.show),
      p(b("Error ID: "), evt.error.uuid.show),
      p(b("Policy: "), evt.actionInfo.actionParams.retry.policy[F].show),
      p(b("Took: "), fmt.format(evt.took)),
      retriesText(evt.numRetries),
      notesText(evt.notes),
      causeText(evt.error)
    )

  private def actionSucced(evt: ActionSucc): Text.TypedTag[String] =
    div(
      h3(evt.actionParams.succedTitle),
      timestampText(evt.zonedDateTime),
      hostServiceText(evt.serviceParams),
      p(b(s"${evt.actionParams.catalog} ID: "), evt.actionInfo.uniqueId.show),
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
