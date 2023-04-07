package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.AlertLevel
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent, Snapshot}
import io.circe.Json
import org.typelevel.cats.time.instances.all

import java.time.Duration

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

  private def metricsSection(snapshot: MetricSnapshot): KeyValueSection = {
    val counters: List[Snapshot.Counter] = snapshot.counters.filter(_.count > 0)
    if (counters.isEmpty) KeyValueSection(CONSTANT_METRICS, "`No updates`")
    else {
      val measures = counters.map(c => c.metricId -> numFmt.format(c.count)) :::
        snapshot.gauges.map(g => g.metricId -> g.value.spaces2)
      val body = measures.sortBy(_._1.metricName).map { case (id, v) => s"${id.show} = $v" }
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
            MarkdownSection(s":rocket: *${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_UPTIME, fmt.format(evt.upTime)),
              second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show)),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}")
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
      )
    )

  private def servicePanic(evt: ServicePanic): SlackApp = {
    val (time, dur) = localTimeAndDurationStr(evt.timestamp, evt.restartTime)
    val msg = s":alarm: The service experienced a panic. Restart was scheduled at *$time*, roughly in $dur."
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(msg),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*$CONSTANT_UPTIME:* ${fmt.format(evt.upTime)}
                                |*$CONSTANT_POLICY:* ${evt.serviceParams.restartPolicy}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}""".stripMargin),
            KeyValueSection("Cause", s"```${abbreviate(evt.error.stackTrace)}```")
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
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
            MarkdownSection(s":octagonal_sign: *${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*$CONSTANT_UPTIME:* ${fmt.format(evt.upTime)}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}
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
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_UPTIME, fmt.format(evt.upTime)),
              second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show)),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}"),
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
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_UPTIME, fmt.format(evt.upTime)),
              second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show)),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}"),
            metricsSection(evt.snapshot)
          )
        ))
    )

  private def instantAlert(evt: InstantAlert): SlackApp = {
    val title = evt.alertLevel match {
      case AlertLevel.Error => ":warning: Error"
      case AlertLevel.Warn  => ":warning: Warning"
      case AlertLevel.Info  => ":information_source: Info"
    }
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*$title:* ${evt.metricName.show}"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}"),
            MarkdownSection(abbreviate(evt.message))
          )
        ))
    )
  }

  private def traceId(evt: ActionEvent): String    = s"*$CONSTANT_TRACE_ID:* ${evt.traceId}"
  private def actionId(evt: ActionEvent): String   = s"*$CONSTANT_ACTION_ID:* ${evt.actionId}"
  private def serviceId(evt: ActionEvent): String  = s"*$CONSTANT_SERVICE_ID:* ${evt.serviceId.show}"
  private def took(evt: ActionResultEvent): String = s"*$CONSTANT_TOOK:* ${fmt.format(evt.took)}"
  private def policy(evt: ActionEvent): String     = s"*$CONSTANT_POLICY:* ${evt.actionParams.retryPolicy}"

  private def actionStart(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${actionTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|${actionId(evt)}
                                |${traceId(evt)}
                                |${serviceId(evt)}""".stripMargin)
          )
        ))
    )

  private def actionRetrying(evt: ActionRetry): SlackApp = {
    val resumeTime = evt.timestamp.plusNanos(evt.delay.toNanos)
    val next       = fmt.format(Duration.between(evt.timestamp, resumeTime))
    val localTs    = resumeTime.toLocalTime

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${actionTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*${toOrdinalWords(evt.retriesSoFar + 1)}* retry at $localTs, in $next
                                |${policy(evt)}
                                |${actionId(evt)}
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection("Cause", s"""```${abbreviate(evt.error.message)}```""")
          )
        ))
    )
  }

  private def actionFailed(evt: ActionFail): SlackApp = {
    val msg: String = s"""|${evt.error.message}
                          |Notes:
                          |${evt.output.spaces2}""".stripMargin

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${actionTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|${took(evt)}
                                |${policy(evt)}
                                |${actionId(evt)}
                                |${traceId(evt)}
                                |${serviceId(evt)}""".stripMargin),
            MarkdownSection(s"""```${abbreviate(msg)}```""".stripMargin)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
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
            MarkdownSection(s"*${actionTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|${took(evt)}
                                |${actionId(evt)}
                                |${traceId(evt)}
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection("Result", s"""```${abbreviate(evt.output.spaces2)}```""")
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
      .withInstantAlert(instantAlert)
      .withActionStart(actionStart)
      .withActionRetry(actionRetrying)
      .withActionFail(actionFailed)
      .withActionComplete(actionCompleted)
}
