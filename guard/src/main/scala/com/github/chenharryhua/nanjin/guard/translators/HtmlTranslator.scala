package com.github.chenharryhua.nanjin.guard.translators

import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.{Importance, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZonedDateTime

/** https://com-lihaoyi.github.io/scalatags/
  */
object HtmlTranslator extends all {
  private def timestampText(timestamp: ZonedDateTime): Text.TypedTag[String] =
    p(b("timestamp: "), localTimestampStr(timestamp))

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("number of retries: "), numRetry.toString)

  private def hostServiceText(si: ServiceParams): Text.TypedTag[String] =
    p(b("service: "), si.name.value, "    ", b("host: "), si.taskParams.hostName)

  private def notesText(n: Notes): Text.TypedTag[String]      = p(b("notes: "), pre(n.value))
  private def causeText(c: NJError): Text.TypedTag[String]    = p(b("cause: "), pre(c.stackTrace))
  private def brief(si: ServiceParams): Text.TypedTag[String] = p(b("brief: "), pre(si.brief))

  private def serviceStatus(ss: ServiceStatus): Text.TypedTag[String] =
    if (ss.isUp) p(b("service is up")) else p(b(style := "color:red")("service is down"))

  private def pendingActions(as: List[PendingAction], now: ZonedDateTime): Text.TypedTag[String] = {
    val tds = "border: 1px solid #dddddd; text-align: left; padding: 8px;"
    div(
      b("pending critical actions:"),
      table(style := "font-family: arial, sans-serif; border-collapse: collapse; width: 100%;")(
        tr(
          th(style := tds)("name"),
          th(style := tds)("so far took"),
          th(style := tds)("launch time"),
          th(style := tds)("id")),
        as.map(a =>
          tr(
            td(style := tds)(a.name.value),
            td(style := tds)(fmt.format(a.launchTime, now)),
            td(style := tds)(localTimestampStr(a.launchTime)),
            td(style := tds)(a.uuid.show)
          ))
      )
    )
  }

  // events

  private def serviceStarted(ss: ServiceStart): Text.TypedTag[String] =
    div(h3(s"Service Started"), timestampText(ss.timestamp), hostServiceText(ss.serviceParams))

  private def servicePanic[F[_]: Applicative](sp: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(s"Service Panic"),
      timestampText(sp.timestamp),
      hostServiceText(sp.serviceParams),
      p(b("restart so far: "), sp.retryDetails.retriesSoFar),
      p(b("error ID: "), sp.error.uuid.show),
      p(b("policy: "), sp.serviceParams.retry.policy[F].show),
      brief(sp.serviceParams),
      causeText(sp.error)
    )

  private def serviceStopped(ss: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := "color:blue")(s"Service Stopped"),
      timestampText(ss.timestamp),
      hostServiceText(ss.serviceParams),
      pre(ss.snapshot.show)
    )

  private def metricsReport(mr: MetricsReport): Text.TypedTag[String] = {
    val color: String = if (mr.snapshot.isContainErrors) "color:red" else "color:black"
    div(
      h3(style := color)(mr.reportType.show),
      serviceStatus(mr.serviceStatus),
      hostServiceText(mr.serviceParams),
      p(b("up time: "), fmt.format(mr.upTime)),
      pendingActions(mr.pendings, mr.timestamp),
      brief(mr.serviceParams),
      pre(mr.snapshot.show)
    )
  }

  private def metricsReset(ms: MetricsReset): Text.TypedTag[String] = {
    val color: String = if (ms.snapshot.isContainErrors) "color:red" else "color:black"
    div(
      h3(style := color)(ms.resetType.show),
      serviceStatus(ms.serviceStatus),
      hostServiceText(ms.serviceParams),
      brief(ms.serviceParams),
      pre(ms.snapshot.show)
    )
  }

  private def serviceAlert(sa: ServiceAlert): Text.TypedTag[String] =
    div(
      h3("Service Alert"),
      timestampText(sa.timestamp),
      hostServiceText(sa.serviceParams),
      p(b("name: "), sa.name.value, "    ", b("importance: "), sa.importance.show),
      pre(sa.message)
    )

  private def actionStart(as: ActionStart): Text.TypedTag[String] =
    div(
      h3(as.actionParams.startTitle),
      timestampText(as.timestamp),
      hostServiceText(as.serviceParams),
      p(b(s"${as.actionInfo.actionParams.alias} ID: "), as.actionInfo.uuid.show)
    )

  private def actionRetrying[F[_]: Applicative](ar: ActionRetry): Text.TypedTag[String] =
    div(
      h3(ar.actionParams.retryTitle),
      timestampText(ar.timestamp),
      hostServiceText(ar.serviceParams),
      p(b(s"${ar.actionInfo.actionParams.alias} ID: "), ar.actionInfo.uuid.show),
      p(b("policy: "), ar.actionInfo.actionParams.retry.policy[F].show)
    )

  private def actionFailed[F[_]: Applicative](af: ActionFail): Option[Text.TypedTag[String]] =
    if (af.actionParams.importance >= Importance.Medium)
      Some(
        div(
          h3(style := "color:red")(af.actionParams.failedTitle),
          timestampText(af.timestamp),
          hostServiceText(af.serviceParams),
          p(b(s"${af.actionParams.alias} ID: "), af.actionInfo.uuid.show),
          p(b("error ID: "), af.error.uuid.show),
          p(b("policy: "), af.actionInfo.actionParams.retry.policy[F].show),
          p(b("took: "), fmt.format(af.took)),
          retriesText(af.numRetries),
          notesText(af.notes),
          brief(af.serviceParams),
          causeText(af.error)
        ))
    else None

  private def actionSucced(as: ActionSucc): Text.TypedTag[String] =
    div(
      h3(as.actionParams.succedTitle),
      timestampText(as.timestamp),
      hostServiceText(as.serviceParams),
      p(b(s"${as.actionParams.alias} ID: "), as.actionInfo.uuid.show),
      p(b("took: "), fmt.format(as.took)),
      retriesText(as.numRetries),
      notesText(as.notes)
    )

  def apply[F[_]: Monad](): Translator[F, Text.TypedTag[String]] =
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
