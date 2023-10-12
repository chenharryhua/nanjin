package com.github.chenharryhua.nanjin.guard.translators

import cats.syntax.all.*
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.guard.config.{AlertLevel, MetricName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, NJEvent}
import io.circe.Json
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.all
import squants.information.{Bytes, Information}

private object SlackTranslator extends all {
  import NJEvent.*
  import textConstants.*
  import textHelper.*

  private def coloring(evt: NJEvent): String = ColorScheme
    .decorate(evt)
    .run {
      case ColorScheme.GoodColor  => Eval.now("#36a64f")
      case ColorScheme.InfoColor  => Eval.now("#b3d1ff")
      case ColorScheme.WarnColor  => Eval.now("#ffd79a")
      case ColorScheme.ErrorColor => Eval.now("#935252")
    }
    .value

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  private val MessageSizeLimits: Information = Bytes(2500)

  private def abbreviate(msg: String): String = StringUtils.abbreviate(msg, MessageSizeLimits.toBytes.toInt)
  private def abbreviate(msg: Json): String   = abbreviate(msg.spaces2)

  private def hostServiceSection(sp: ServiceParams): JuxtaposeSection = {
    val sn: String =
      sp.taskParams.homePage.fold(sp.serviceName)(hp => s"<$hp|${sp.serviceName}>")
    JuxtaposeSection(TextField(CONSTANT_SERVICE, sn), TextField(CONSTANT_HOST, sp.taskParams.hostName.value))
  }
  private def upTimeSection(evt: NJEvent): JuxtaposeSection =
    JuxtaposeSection(
      first = TextField(CONSTANT_UPTIME, upTimeText(evt)),
      second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show))

  private def metricsSection(snapshot: MetricSnapshot): KeyValueSection = {
    val yaml = new SnapshotPolyglot(snapshot).counterYaml match {
      case Some(value) => s"""```${abbreviate(value)}```"""
      case None        => "`No updates`"
    }
    KeyValueSection(CONSTANT_METRICS, yaml)
  }

  private def brief(json: Json): KeyValueSection =
    KeyValueSection(CONSTANT_BRIEF, s"```${abbreviate(json)}```")

// events
  private def serviceStarted(evt: ServiceStart): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s":rocket: *${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}""".stripMargin)
          )
        )) ++ evt.serviceParams.brief.map(bf => Attachment(color = color, blocks = List(brief(bf))))
    )
  }

  private def servicePanic(evt: ServicePanic): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(":alarm:" + panicText(evt)),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"""|*$CONSTANT_POLICY:* ${evt.serviceParams.servicePolicies.restart}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}""".stripMargin)
          )
        ),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${abbreviate(evt.error.stackTrace)}```")))
      ) ++ evt.serviceParams.brief.map(bf => Attachment(color = color, blocks = List(brief(bf))))
    )
  }

  private def serviceStopped(evt: ServiceStop): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s":octagonal_sign: *${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"""|*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}
                                |*$CONSTANT_CAUSE:* ${abbreviate(evt.cause.show)}""".stripMargin)
          )
        )
      ) ++ evt.serviceParams.brief.map(bf => Attachment(color = color, blocks = List(brief(bf))))
    )
  }

  private def metricReport(evt: MetricReport): SlackApp = {
    val color = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"),
            metricsSection(evt.snapshot)
          )
        )
      ) ++ evt.serviceParams.brief.map(bf => Attachment(color = color, blocks = List(brief(bf))))
    )
  }

  private def metricReset(evt: MetricReset): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"),
            metricsSection(evt.snapshot)
          )
        ))
    )

  private def measurement(mn: MetricName): String = s"*$CONSTANT_MEASUREMENT:* ${mn.measurement}"

  private def serviceAlert(evt: ServiceAlert): SlackApp = {
    val symbol: String = evt.alertLevel match {
      case AlertLevel.Error => ":warning:"
      case AlertLevel.Warn  => ":warning:"
      case AlertLevel.Info  => ":information_source:"
    }
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(symbol + s" *${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            upTimeSection(evt),
            MarkdownSection(s"""|${measurement(evt.metricName)}
                                |*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}""".stripMargin),
            MarkdownSection(s"```${abbreviate(evt.message)}```")
          )
        ))
    )
  }

  private def serviceId(evt: ActionEvent): String =
    s"*$CONSTANT_SERVICE_ID:* ${evt.serviceParams.serviceId.show}"
  private def policy(evt: ActionEvent): String = s"*$CONSTANT_POLICY:* ${evt.actionParams.retryPolicy}"

  private def actionStart(evt: ActionStart): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId.show),
              second = TextField(CONSTANT_TIMEZONE, evt.serviceParams.taskParams.zoneId.show)),
            MarkdownSection(s"""|${measurement(evt.actionParams.metricName)}
                                |${policy(evt)}
                                |${serviceId(evt)}""".stripMargin)
          ) ++ evt.notes.map(js => MarkdownSection(s"""```${abbreviate(js)}```"""))
        ))
    )

  private def actionRetrying(evt: ActionRetry): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId.show),
              second = TextField(CONSTANT_SNOOZE, fmt.format(evt.tick.snooze))),
            MarkdownSection(s"""|${retryText(evt)}
                                |${policy(evt)}
                                |${measurement(evt.actionParams.metricName)}
                                |${serviceId(evt)}""".stripMargin),
            KeyValueSection(CONSTANT_CAUSE, s"""```${abbreviate(evt.error.message)}```""")
          )
        ))
    )

  private def actionFailed(evt: ActionFail): SlackApp = {
    val color: String = coloring(evt)
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = color,
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId.show),
              second = TextField(CONSTANT_TOOK, tookText(evt))),
            MarkdownSection(s"""|${policy(evt)}
                                |${measurement(evt.actionParams.metricName)}
                                |${serviceId(evt)}""".stripMargin)
          ) ++ evt.notes.map(js => MarkdownSection(s"""```${abbreviate(js)}```"""))
        ),
        Attachment(
          color = color,
          blocks = List(KeyValueSection(CONSTANT_CAUSE, s"```${abbreviate(evt.error.stackTrace)}```")))
      ) ++ evt.serviceParams.brief.map(bf => Attachment(color = color, blocks = List(brief(bf))))
    )
  }

  private def actionDone(evt: ActionDone): SlackApp =
    SlackApp(
      username = evt.serviceParams.taskParams.taskName,
      attachments = List(
        Attachment(
          color = coloring(evt),
          blocks = List(
            MarkdownSection(s"*${eventTitle(evt)}*"),
            hostServiceSection(evt.serviceParams),
            JuxtaposeSection(
              first = TextField(CONSTANT_ACTION_ID, evt.actionId.show),
              second = TextField(CONSTANT_TOOK, tookText(evt))),
            MarkdownSection(s"""|${measurement(evt.actionParams.metricName)}
                                |${serviceId(evt)}""".stripMargin)
          ) ++ evt.notes.map(js => MarkdownSection(s"""```${abbreviate(js)}```"""))
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
      .withActionDone(actionDone)
}
