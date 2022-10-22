package com.github.chenharryhua.nanjin.guard.translators

import cats.{Applicative, Eval}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJEvent}
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

  private def metricsSection(snapshot: MetricSnapshot): KeyValueSection =
    KeyValueSection("Metrics", s"```${abbreviate(snapshot.show.replace("-- ", ""))}```")

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
            MarkdownSection(s"*Service ID:* ${evt.serviceId.show}"),
            MarkdownSection(evt.serviceParams.brief)
          )
        ))
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
        )
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
        )
      )
    )

  private def metricIndex(index: MetricIndex): String = index match {
    case MetricIndex.Adhoc           => "(Adhoc)"
    case MetricIndex.Periodic(index) => s"(index=$index)"
  }

  private def metricReport(evt: MetricReport): SlackApp = {
    val nextReport =
      evt.serviceParams.metricParams.nextReport(evt.timestamp).map(_.toLocalTime.show).getOrElse("none")

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title + metricIndex(evt.index)}*"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Next Report:* $nextReport
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
            metricsSection(evt.snapshot)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(MarkdownSection(evt.serviceParams.brief)))
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
            MarkdownSection(s"*${evt.title + metricIndex(evt.index)}*"),
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
    val title = evt.importance match {
      case Importance.Critical => ":warning: Error"
      case Importance.High     => ":warning: Warning"
      case Importance.Medium   => ":information_source: Info"
      case Importance.Low      => "oops. should not happen"
    }
    val msg: Option[Section] =
      if (evt.message.nonEmpty) Some(MarkdownSection(abbreviate(evt.message))) else None
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*$title:* ${evt.digested.metricRepr}"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*Service ID:* ${evt.serviceId.show}")
          ).appendedAll(msg)
        )
      )
    )
  }

  private def traceId(evt: ActionEvent): String = s"${evt.traceId.getOrElse("none")}"

  private def actionStart(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField("Name", evt.digested.metricRepr),
              second = TextField("ID", evt.actionId.show)),
            MarkdownSection(s"""|*Trace ID:* ${traceId(evt)}
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
            KeyValueSection("Input", s"""```${abbreviate(evt.actionInfo.input.spaces2)}```""")
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
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField("Name", evt.digested.metricRepr),
              second = TextField("ID", evt.actionId.show)),
            MarkdownSection(s"""|*Took so far:* ${fmt.format(evt.took)}
                                |*${toOrdinalWords(evt.retriesSoFar + 1)}* retry at $localTs, in $next
                                |*Policy:* ${evt.actionParams.retryPolicy}
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
            KeyValueSection("Cause", s"""```${abbreviate(evt.error.message)}```""")
          )
        ))
    )
  }

  private def actionFailed(evt: ActionFail): SlackApp = {
    val msg: String = s"""|${evt.error.message}
                          |Input:
                          |${evt.actionInfo.input.spaces2}""".stripMargin

    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField("Name", evt.digested.metricRepr),
              second = TextField("ID", evt.actionId.show)),
            MarkdownSection(s"""|*Took:* ${fmt.format(evt.took)}
                                |*Policy:* ${evt.actionParams.retryPolicy}
                                |*Trace ID:* ${traceId(evt)}
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
            MarkdownSection(s"""```${abbreviate(msg)}```""".stripMargin)
          )
        )
      )
    )
  }

  private def actionSucced(evt: ActionSucc): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField("Name", evt.digested.metricRepr),
              second = TextField("ID", evt.actionId.show)),
            MarkdownSection(s"""|*Took:* ${fmt.format(evt.took)}
                                |*Trace ID:* ${traceId(evt)}
                                |*Service ID:* ${evt.serviceId.show}""".stripMargin),
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
      .withActionSucc(actionSucced)
}
