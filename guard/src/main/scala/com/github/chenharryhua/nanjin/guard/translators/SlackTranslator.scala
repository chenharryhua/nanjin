package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.{AlertLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent, Snapshot}
import io.circe.Json
import org.typelevel.cats.time.instances.all

import java.time.Duration
import java.time.temporal.ChronoUnit

private object SlackTranslator extends all {
  import NJEvent.*

  private def coloring(evt: NJEvent): String = ColorScheme
    .decorate(evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now("#36a64f")
      case ColorScheme.InfoColor  => Eval.now("#b3d1ff")
      case ColorScheme.WarnColor  => Eval.now("#ffd79a")
      case ColorScheme.ErrorColor => Eval.now("#935252")
    }
    .value

  private def hostServiceSection(sp: ServiceParams): JuxtaposeSection = {
    val sn: String =
      sp.homePage.fold(sp.serviceName.value)(hp => s"<${hp.value}|${sp.serviceName.value}>")
    JuxtaposeSection(TextField(CONSTANT_SERVICE, sn), TextField(CONSTANT_HOST, sp.taskParams.hostName.value))
  }
  private def upTimeSection(evt: NJEvent): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_UPTIME, fmt.format(evt.upTime)),
      second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show))

  private def metricsSection(snapshot: MetricSnapshot): KeyValueSection = {
    val counters: List[Snapshot.Counter] = snapshot.counters.filter(_.count > 0)
    if (counters.isEmpty) KeyValueSection(CONSTANT_METRICS, "`No updates`")
    else {
      val measures = counters.map(c => c.metricId -> numFmt.format(c.count)) :::
        snapshot.gauges.map(g => g.metricId -> g.value.spaces2)
      val body = measures.sortBy(_._1.metricName).map { case (id, v) => s"${id.display} = $v" }
      KeyValueSection(CONSTANT_METRICS, s"""```${abbreviate(body.mkString("\n"))}```""")
    }
  }

  private def brief(json: Json): KeyValueSection =
    KeyValueSection(CONSTANT_BRIEF, s"```${abbreviate(json.spaces2)}```")

// events
  private def serviceStarted(evt: ServiceStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s":rocket: *${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}")
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
      )
    )

  private def servicePanic(evt: ServicePanic): SlackApp = {
    val color       = coloring(evt)
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg = s":alarm: The service experienced a panic. Restart was scheduled at *$time*, roughly in $dur."
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(msg),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(
              s"""|*$CONSTANT_POLICY:* ${evt.serviceParams.restartPolicy}
                  |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}""".stripMargin)
          )
        ),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${abbreviate(evt.error.stackTrace)}```"))),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def serviceStopped(evt: ServiceStop): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s":octagonal_sign: *${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"""|*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}
                                |*$CONSTANT_CAUSE:* ${abbreviate(evt.cause.show)}""".stripMargin)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
      )
    )

  private def metricReport(evt: MetricReport): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}"),
            metricsSection(evt.snapshot)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
      )
    )

  private def metricReset(evt: MetricReset): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}"),
            metricsSection(evt.snapshot)
          )
        ))
    )

  private def serviceAlert(evt: ServiceAlert): SlackApp = {
    val symbol: String = evt.alertLevel match {
      case AlertLevel.Error => ":warning:"
      case AlertLevel.Warn  => ":warning:"
      case AlertLevel.Info  => ":information_source:"
    }
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(symbol + s" *${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}"),
            MarkdownSection(s"```${abbreviate(evt.message.spaces2)}```")
          )
        ))
    )
  }

  private def traceId(evt: ActionEvent): String = s"*$CONSTANT_TRACE_ID:* ${evt.traceId}"
  private def serviceId(evt: ActionEvent): String =
    s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.value.show}"
  private def policy(evt: ActionEvent): String = s"*$CONSTANT_POLICY:* ${evt.actionParams.retryPolicy}"

  private def actionStart(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId),
              second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show)),
            MarkdownSection(s"""|${traceId(evt)}
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection(CONSTANT_INPUT, s"""```${abbreviate(evt.json.spaces2)}```""".stripMargin)
          )
        ))
    )

  private def actionRetrying(evt: ActionRetry): SlackApp = {
    val resumeTime = evt.timestamp.plusNanos(evt.delay.toNanos)
    val next       = fmt.format(Duration.between(evt.timestamp, resumeTime))
    val localTs    = resumeTime.toLocalTime.truncatedTo(ChronoUnit.SECONDS)

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId),
              second = TextField(CONSTANT_DELAYED, fmt.format(evt.tookSoFar))),
            MarkdownSection(s"""|*${toOrdinalWords(evt.retriesSoFar + 1)}* retry at $localTs, in $next
                                |${policy(evt)}
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection(CONSTANT_CAUSE, s"""```${abbreviate(evt.error.message)}```""")
          )
        ))
    )
  }

  private def actionFailed(evt: ActionFail): SlackApp = {
    val color: String = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId),
              second = TextField(CONSTANT_TOOK, fmt.format(evt.took))),
            MarkdownSection(s"""|${policy(evt)}
                                |${traceId(evt)}
                                |${serviceId(evt)}""".stripMargin)
          )
        ),
        Attachment(
          color = color,
          blocks =
            List(KeyValueSection(CONSTANT_INPUT, s"""```${abbreviate(evt.json.spaces2)}```""".stripMargin))),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${abbreviate(evt.error.stackTrace)}```"))),
        Attachment(color = color, blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def actionCompleted(evt: ActionComplete): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId),
              second = TextField(CONSTANT_TOOK, fmt.format(evt.took))),
            MarkdownSection(s"""|${traceId(evt)}
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection("Result", s"""```${abbreviate(evt.json.spaces2)}```""")
          )
        )
      )
    )

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
      .withServiceStart(serviceStarted)
      .withServicePanic(servicePanic)
      .withServiceStop(serviceStopped)
      .withMetricReport(metricReport)
      .withMetricReset(metricReset)
      .withServiceAlert(serviceAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionComplete(actionCompleted)
}
