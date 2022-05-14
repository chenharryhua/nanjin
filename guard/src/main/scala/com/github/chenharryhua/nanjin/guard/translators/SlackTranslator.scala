package com.github.chenharryhua.nanjin.guard.translators

import cats.Applicative
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{DurationFormatter, NJLocalTime, NJLocalTimeRange}
import com.github.chenharryhua.nanjin.guard.config.Importance
import com.github.chenharryhua.nanjin.guard.event.*
import io.circe.generic.auto.*
import org.typelevel.cats.time.instances.all

import java.text.NumberFormat

private object SlackTranslator extends all {

  private val coloring: Coloring = new Coloring({
    case ColorScheme.GoodColor  => "#36a64f"
    case ColorScheme.InfoColor  => "#b3d1ff"
    case ColorScheme.WarnColor  => "#ffd79a"
    case ColorScheme.ErrorColor => "#935252"
  })

  private def metricsSection(snapshot: MetricSnapshot): KeyValueSection =
    if (snapshot.show.length <= MessageSizeLimits) {
      KeyValueSection("Metrics", s"```${snapshot.show.replace("-- ", "")}```")
    } else {
      val fmt: NumberFormat = NumberFormat.getIntegerInstance
      val msg: String =
        snapshot.counterMap.filter(_._2 > 0).map(x => s"${x._1}: ${fmt.format(x._2)}").toList.sorted.mkString("\n")
      if (msg.isEmpty)
        KeyValueSection("Counters", "*No counter update*")
      else
        KeyValueSection("Counters", s"```${abbreviate(msg)}```")
    }
  // don't trim string
  private def noteSection(notes: Notes): Option[MarkdownSection] =
    if (notes.value.isEmpty) None else Some(MarkdownSection(abbreviate(notes.value)))

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
            MarkdownSection(s"*Service ID:* ${evt.serviceID.show}"),
            MarkdownSection(evt.serviceParams.brief)
          )
        ))
    )

  private def servicePanic[F[_]: Applicative](evt: ServicePanic): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(upcomingRestartTimeInterpretation(evt)),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"""|*Up Time:* ${fmt.format(evt.upTime)}
                                |*Restart Policy:* ${evt.serviceParams.retry.policy[F].show}
                                |*Error ID:* ${evt.error.uuid.show}
                                |*Service ID:* ${evt.serviceID.show}""".stripMargin),
            KeyValueSection("Cause", s"```${abbreviate(evt.error.stackTrace)}```")
          )
        )
      )
    )

  private def serviceStopped(evt: ServiceStop): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s":octagonal_sign: *${evt.title}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              TextField("Up Time", fmt.format(evt.upTime)),
              TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)),
            MarkdownSection(s"""*Service ID:* ${evt.serviceID.show}
                               |*Cause:* ${evt.cause.show}""".stripMargin)
          )
        )
      )
    )

  private def instantAlert(evt: InstantAlert): SlackApp = {
    val title = evt.importance match {
      case Importance.Critical => ":warning: Error"
      case Importance.High     => ":warning: Warning"
      case Importance.Medium   => ":information_source: Info"
      case Importance.Low      => "oops. should not happen"
    }
    val msg: Option[Section] = if (evt.message.nonEmpty) Some(MarkdownSection(abbreviate(evt.message))) else None
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*$title:* ${evt.metricName.metricRepr}"),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*Service ID:* ${evt.serviceID.show}")
          ).appendedAll(msg)
        )
      )
    )
  }

  private def metricReport(evt: MetricReport): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"""*${evt.title}*
                               |${upcomingRestartTimeInterpretation(evt)}""".stripMargin),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show),
              TextField(
                "Scheduled Next",
                evt.serviceParams.metric
                  .nextReport(evt.timestamp)
                  .map(next => localTimeAndDurationStr(evt.timestamp, next)._1)
                  .getOrElse("None"))
            ),
            MarkdownSection(s"*Service ID:* ${evt.serviceID.show}"),
            metricsSection(evt.snapshot)
          )
        ),
        Attachment(color = coloring(evt), blocks = List(MarkdownSection(evt.serviceParams.brief)))
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
              TextField("Up Time", fmt.format(evt.upTime)),
              TextField("Time Zone", evt.serviceParams.taskParams.zoneId.show)
            ),
            MarkdownSection(s"*Service ID:* ${evt.serviceID.show}"),
            metricsSection(evt.snapshot)
          )
        )
      )
    )

  private def actionStart(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            JuxtaposeSection(
              TextField("Name", evt.metricName.metricRepr),
              TextField("ID", evt.actionInfo.actionID.show)
            ),
            hostServiceSection(evt.serviceParams),
            MarkdownSection(s"*Service ID:* ${evt.serviceID.show}".stripMargin)
          )
        ))
    )

  private def actionRetrying[F[_]: Applicative](evt: ActionRetry): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            JuxtaposeSection(
              TextField("Name", evt.metricName.metricRepr),
              TextField("ID", evt.actionInfo.actionID.show)
            ),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              TextField("Took so far", fmt.format(evt.took)),
              TextField("Retries so far", evt.willDelayAndRetry.retriesSoFar.show)),
            MarkdownSection(s"""*Next retry in:* ${fmt.format(evt.willDelayAndRetry.nextDelay)}
                               |*Policy:* ${evt.actionParams.retry.policy[F].show}
                               |*Service ID:* ${evt.serviceID.show}""".stripMargin),
            KeyValueSection("Cause", s"```${evt.error.message}```")
          )
        ))
    )

  private def actionFailed[F[_]: Applicative](evt: ActionFail): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            JuxtaposeSection(
              TextField("Name", evt.metricName.metricRepr),
              TextField("ID", evt.actionInfo.actionID.show)
            ),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(TextField("Took", fmt.format(evt.took)), TextField("Retries", evt.numRetries.show)),
            MarkdownSection(s"""*Policy:* ${evt.actionParams.retry.policy[F].show}
                               |*Service ID:* ${evt.serviceID.show}""".stripMargin)
          ).appendedAll(noteSection(evt.notes))
        )
      )
    )

  private def actionSucced(evt: ActionSucc): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName.value,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${evt.title}*"),
            JuxtaposeSection(
              TextField("Name", evt.metricName.metricRepr),
              TextField("ID", evt.actionInfo.actionID.show)
            ),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(TextField("Took", fmt.format(evt.took)), TextField("Retries", evt.numRetries.show)),
            MarkdownSection(s"*Service ID:* ${evt.serviceID.show}".stripMargin)
          ).appendedAll(noteSection(evt.notes))
        )
      )
    )

  def apply[F[_]: Applicative]: Translator[F, SlackApp] =
    Translator
      .empty[F, SlackApp]
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
