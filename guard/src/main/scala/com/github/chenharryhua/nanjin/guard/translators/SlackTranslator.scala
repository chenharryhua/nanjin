package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.AlertLevel
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
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
    val counters = snapshot.counters
      .filter(_.count > 0)
      .map(c => f"${c.digested.show}.${c.category} = ${c.count}%d")
      .sorted
    val gauges = snapshot.gauges.map(g => s"${g.digested.show}.gauge = ${g.value}")
    val text   = abbreviate((counters ::: gauges).mkString("\n"))
    KeyValueSection("Metrics", if (text.isEmpty) "`No updates`" else s"```$text```")
  }

  private def brief(json: Json): KeyValueSection =
    KeyValueSection("Brief", s"```${abbreviate(json.spaces2)}```")

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
              first = TextField("Up Time", fmt.format(evt.upTime)),
              second = TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)
            ),
            MarkdownSection(s"*Service ID:* ${evt.serviceId.show}")
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
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Policy:* ${evt.serviceParams.retryPolicy}
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
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
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Service ID:* ${evt.serviceId.show}
                                |*Cause:* ${evt.cause.show}""".stripMargin)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
      )
    )

  private def metricReport(evt: MetricReport): SlackApp = {
    val nextReport =
      evt.serviceParams.metricParams.nextReport(evt.timestamp).map(_.toLocalTime.show).getOrElse("none")

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Next Report:* $nextReport
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
            metricsSection(evt.snapshot)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(brief(evt.serviceParams.brief)))
      )
    )
  }

  private def metricReset(evt: MetricReset): SlackApp = {
    val nextReset =
      evt.serviceParams.metricParams.nextReset(evt.timestamp).map(_.toLocalDateTime.show).getOrElse("none")

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Next Reset:* $nextReset
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
            metricsSection(evt.snapshot)
          )
        )
      )
    )
  }

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
            MarkdownSection(s"*$title:* ${evt.digested.show}"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*Service ID:* ${evt.serviceId.show}"),
            MarkdownSection(abbreviate(evt.message))
          )
        )
      )
    )
  }

  private def traceId(evt: ActionEvent): String    = s"*Trace ID:* ${evt.traceId}"
  private def actionId(evt: ActionEvent): String   = s"*Action ID:* ${evt.actionId}"
  private def serviceId(evt: ActionEvent): String  = s"*Service ID:* ${evt.serviceId.show}"
  private def took(evt: ActionResultEvent): String = s"*Took:* ${fmt.format(evt.took)}"
  private def policy(evt: ActionEvent): String     = s"*Policy:* ${evt.actionParams.retryPolicy}"

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
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection("Input", s"""```${abbreviate(evt.input.spaces2)}```""")
          )
        ))
    )

  private def actionRetrying(evt: ActionRetry): SlackApp = {
    val next    = fmt.format(Duration.between(evt.timestamp, evt.resumeTime))
    val localTs = evt.resumeTime.toLocalTime

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
                          |Input:
                          |${evt.input.spaces2}""".stripMargin

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
            KeyValueSection("Output", s"""```${abbreviate(evt.output.spaces2)}```""")
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
