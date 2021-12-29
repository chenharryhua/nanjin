package com.github.chenharryhua.nanjin.guard.observers

import cats.implicits.{catsSyntaxApplicative, catsSyntaxApplicativeError, toFunctorOps, toTraverseOps}
import cats.syntax.all.*
import cats.{Applicative, Monad}
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.event.*
import org.typelevel.cats.time.instances.all
import scalatags.Text
import scalatags.Text.all.*

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

/** https://com-lihaoyi.github.io/scalatags/
  */
private[observers] object DefaultEmailTranslator extends all {
  private def timestampText(timestamp: ZonedDateTime): Text.TypedTag[String] =
    p(b("timestamp: "), timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show)
  private val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  private def retriesText(numRetry: Int): Text.TypedTag[String] =
    p(b("number of retries: "), numRetry.toString)

  private def hostServiceText(si: ServiceInfo): Text.TypedTag[String] =
    p(b("service: "), si.serviceParams.name.value, "    ", b("host: "), si.serviceParams.taskParams.hostName)

  private def notesText(n: Notes): Text.TypedTag[String]    = p(b("notes: "), pre(n.value))
  private def causeText(c: NJError): Text.TypedTag[String]  = p(b("cause: "), pre(c.stackTrace))
  private def brief(si: ServiceInfo): Text.TypedTag[String] = p(b("brief: ", si.serviceParams.brief))

  private def serviceStarted(ss: ServiceStarted): Text.TypedTag[String] =
    div(h3(s"Service Started"), timestampText(ss.timestamp), hostServiceText(ss.serviceInfo))

  private def servicePanic[F[_]: Applicative](sp: ServicePanic): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(s"Service Panic"),
      timestampText(sp.timestamp),
      hostServiceText(sp.serviceInfo),
      p(b("restart so far: "), sp.retryDetails.retriesSoFar),
      p(b("error ID: "), sp.error.uuid.show),
      p(b("policy: "), sp.serviceInfo.serviceParams.retry.policy[F].show),
      brief(sp.serviceInfo),
      causeText(sp.error)
    )

  private def serviceStopped(ss: ServiceStopped): Text.TypedTag[String] =
    div(
      h3(style := "color:blue")(s"Service Stopped"),
      timestampText(ss.timestamp),
      hostServiceText(ss.serviceInfo),
      pre(ss.snapshot.show)
    )

  private def metricsReport(mr: MetricsReport): Text.TypedTag[String] = {
    val color: String = if (mr.snapshot.isContainErrors) "color:red" else "color:black"
    div(
      h3(style := color)(mr.reportType.show),
      hostServiceText(mr.serviceInfo),
      p(b("up time: "), fmt.format(mr.serviceInfo.launchTime, mr.timestamp)),
      brief(mr.serviceInfo),
      pre(mr.snapshot.show)
    )
  }

  private def metricsReset(ms: MetricsReset): Text.TypedTag[String] = {
    val color: String = if (ms.snapshot.isContainErrors) "color:red" else "color:black"
    div(
      h3(style := color)(ms.resetType.show),
      hostServiceText(ms.serviceInfo),
      brief(ms.serviceInfo),
      pre(ms.snapshot.show)
    )
  }

  private def serviceAlert(sa: ServiceAlert): Text.TypedTag[String] =
    div(
      h3("Service Alert"),
      timestampText(sa.timestamp),
      hostServiceText(sa.serviceInfo),
      p(b("name: "), sa.name.value, "    ", b("importance: "), sa.importance.show),
      pre(sa.message)
    )

  private def actionStart(as: ActionStart): Text.TypedTag[String] =
    div(
      h3(s"${as.actionParams.name.value} Start"),
      timestampText(as.timestamp),
      hostServiceText(as.actionInfo.serviceInfo)
    )

  private def actionRetrying(ar: ActionRetrying): Text.TypedTag[String] =
    div(
      h3(s"${ar.actionParams.name.value} Retrying"),
      timestampText(ar.timestamp),
      hostServiceText(ar.actionInfo.serviceInfo),
      p(b(s"${ar.actionInfo.actionParams.alias} ID: "), ar.actionInfo.uuid.show)
    )

  private def actionFailed[F[_]: Applicative](af: ActionFailed): Text.TypedTag[String] =
    div(
      h3(style := "color:red")(s"${af.actionParams.name.value} Failed"),
      timestampText(af.timestamp),
      hostServiceText(af.actionInfo.serviceInfo),
      p(b(s"${af.actionInfo.actionParams.alias} ID: "), af.actionInfo.uuid.show),
      p(b("error ID: "), af.error.uuid.show),
      p(b("policy: "), af.actionInfo.actionParams.retry.policy[F].show),
      p(b("took: "), fmt.format(af.actionInfo.launchTime, af.timestamp)),
      retriesText(af.numRetries),
      notesText(af.notes),
      brief(af.serviceInfo),
      causeText(af.error)
    )

  private def actionSucced(as: ActionSucced): Text.TypedTag[String] =
    div(
      h3(s"${as.actionParams.name.value} Succed"),
      timestampText(as.timestamp),
      hostServiceText(as.actionInfo.serviceInfo),
      p(b(s"${as.actionInfo.actionParams.alias} ID: "), as.actionInfo.uuid.show),
      p(b("took: "), fmt.format(as.actionInfo.launchTime, as.timestamp)),
      retriesText(as.numRetries),
      notesText(as.notes)
    )

  def apply[F[_]: Monad](): Translator[F, Text.TypedTag[String]] =
    Translator
      .empty[F, Text.TypedTag[String]]
      .withServiceStarted(serviceStarted)
      .withServicePanic(servicePanic[F])
      .withServiceStopped(serviceStopped)
      .withMetricsReport(metricsReport)
      .withMetricsReset(metricsReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetrying(actionRetrying)
      .withActionFailed(actionFailed[F])
      .withActionSucced(actionSucced)
}
