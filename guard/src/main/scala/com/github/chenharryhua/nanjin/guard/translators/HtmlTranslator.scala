package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJError, NJEvent}
import io.circe.Json
import org.typelevel.cats.time.instances.all
import scalatags.Text.all.*
import scalatags.text.Builder
import scalatags.{generic, Text}

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
      evt.serviceParams.homePage.fold(td(evt.serviceName.value))(hp =>
        td(a(href := hp.value)(evt.serviceName.value)))

    frag(
      tr(
        th(CONSTANT_TIMESTAMP),
        th(CONSTANT_SERVICE),
        th(CONSTANT_TASK),
        th(CONSTANT_HOST),
        th(CONSTANT_SERVICE_ID),
        th(CONSTANT_UPTIME)
      ),
      tr(
        td(evt.timestamp.toLocalTime.truncatedTo(ChronoUnit.SECONDS).show),
        serviceName,
        td(evt.serviceParams.taskParams.taskName.value),
        td(evt.serviceParams.taskParams.hostName.value),
        td(evt.serviceId.show),
        td(fmt.format(evt.upTime))
      )
    )
  }

  private def causeText(c: NJError): Text.TypedTag[String] =
    p(b(s"$CONSTANT_CAUSE: "), pre(small(c.stackTrace)))
  private def snapshotText(sp: ServiceParams, ss: MetricSnapshot): Text.TypedTag[String] =
    pre(small(new SnapshotJson(ss).toPrettyJson(sp.metricParams).spaces2))
  private def jsonText(js: Json): Text.TypedTag[String]     = pre(small(js.spaces2))
  private def briefText(brief: Json): Text.TypedTag[String] = pre(small(brief.spaces2))

  // events

  private def serviceStarted(evt: ServiceStart): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      briefText(evt.serviceParams.brief)
    )

  private def servicePanic(evt: ServicePanic): Text.TypedTag[String] = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg         = s"The service experienced a panic. Restart was scheduled at $time, roughly in $dur."
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      p(b(msg)),
      p(b(s"$CONSTANT_POLICY: "), evt.serviceParams.restartPolicy),
      causeText(evt.error)
    )
  }

  private def serviceStopped(evt: ServiceStop): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      p(b(s"$CONSTANT_CAUSE: "), evt.cause.show),
      briefText(evt.serviceParams.brief)
    )

  private def metricReport(evt: MetricReport): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      snapshotText(evt.serviceParams, evt.snapshot)
    )

  private def metricReset(evt: MetricReset): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      briefText(evt.serviceParams.brief),
      snapshotText(evt.serviceParams, evt.snapshot)
    )

  private def instantAlert(evt: InstantAlert): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt)),
      pre(evt.message)
    )

  private def actionStart(evt: ActionStart): Text.TypedTag[String] = {
    val start = frag(
      tr(td(b(CONSTANT_IMPORTANCE)), td(b(CONSTANT_ACTION_ID)), td(b(CONSTANT_TRACE_ID))),
      tr(td(evt.actionParams.importance.show), td(evt.actionId), td(evt.traceId))
    )
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), start)
    )
  }

  private def actionRetrying(evt: ActionRetry): Text.TypedTag[String] = {

    val retry = frag(
      tr(
        td(b(CONSTANT_IMPORTANCE)),
        td(b(CONSTANT_ACTION_ID)),
        td(b(CONSTANT_TRACE_ID)),
        td(b("Index")),
        td(b("Resume"))),
      tr(
        td(evt.actionParams.importance.show),
        td(evt.actionId),
        td(evt.traceId),
        td(evt.retriesSoFar + 1),
        td(evt.timestamp.plusNanos(evt.delay.toNanos).toLocalTime.show)
      )
    )
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), retry),
      p(b(s"$CONSTANT_POLICY: "), evt.actionParams.retryPolicy),
      causeText(evt.error)
    )
  }

  private def actionResultTable(evt: ActionResultEvent): generic.Frag[Builder, String] =
    frag(
      tr(
        td(b(CONSTANT_IMPORTANCE)),
        td(b(CONSTANT_ACTION_ID)),
        td(b(CONSTANT_TRACE_ID)),
        td(b(CONSTANT_TOOK))),
      tr(td(evt.actionParams.importance.show), td(evt.actionId), td(evt.traceId), td(fmt.format(evt.took)))
    )

  private def actionFailed(evt: ActionFail): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), actionResultTable(evt)),
      p(b(s"$CONSTANT_POLICY: "), evt.actionParams.retryPolicy),
      p(b(s"$CONSTANT_NOTES: "), jsonText(evt.output)), // align with slack
      causeText(evt.error)
    )

  private def actionCompleted(evt: ActionComplete): Text.TypedTag[String] =
    div(
      h3(style := coloring(evt))(eventTitle(evt)),
      table(hostServiceTable(evt), actionResultTable(evt)),
      p(b(s"$CONSTANT_RESULT: "), jsonText(evt.output)) // align with slack
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
      .withActionComplete(actionCompleted)
}
