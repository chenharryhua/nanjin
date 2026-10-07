package com.github.chenharryhua.nanjin.guard.translator

import cats.syntax.show.showInterpolator
import cats.{Applicative, Eval}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.{Active, Event, Took}

import scala.io.AnsiColor

object AnsiTextTranslator {
  import Event.*

  private def coloredEventTitle(evt: Event): String =
    eventLogLevel[Eval, String](evt).run {
      case LogLevel.Error => Eval.now(s"${AnsiColor.RED}${eventTitle(evt)}${AnsiColor.RESET}")
      case LogLevel.Warn  => Eval.now(s"${AnsiColor.YELLOW}${eventTitle(evt)}${AnsiColor.RESET}")
      case LogLevel.Good  => Eval.now(s"${AnsiColor.GREEN}${eventTitle(evt)}${AnsiColor.RESET}")
      case LogLevel.Info  => Eval.now(s"${AnsiColor.CYAN}${eventTitle(evt)}${AnsiColor.RESET}")
      case LogLevel.Debug => Eval.now(s"${AnsiColor.MAGENTA}${eventTitle(evt)}${AnsiColor.RESET}")
    }.value

  private def service_event(se: Event): String = {
    val host: String = Labelled(se.serviceIdentity.host).labelledText
    val sn: String = Labelled(se.serviceIdentity.service).labelledText
    val tn: String = Labelled(se.serviceIdentity.task).labelledText
    val sid: String = Labelled(se.serviceIdentity.serviceId).labelledText
    val uptime: String = Labelled(se.upTime).labelledText

    s"""|${coloredEventTitle(se)}
        |$sn, $tn, $uptime
        |  $host
        |  $sid""".stripMargin
  }

  private def service_start(evt: ServiceStart): String = {
    val idx = s"index:${evt.tick.index}"
    val snz = Labelled(Took(evt.tick.snooze)).labelledText

    s"""|${service_event(evt)}
        |  $idx, $snz
        |  ${evt.brief.value.spaces2}
        |""".stripMargin
  }

  private def service_panic(evt: ServicePanic): String = {
    val idx = s"index:${evt.tick.index}"
    val act = Labelled(Active(evt.tick.active)).labelledText

    s"""|${service_event(evt)}
        |  $idx, $act
        |${panicText(evt)}
        |${Labelled(evt.stackTrace).labelledText}
        |""".stripMargin
  }

  private def service_stop(evt: ServiceStop): String =
    s"""|${service_event(evt)}
        |${Labelled(evt.cause).labelledText}
        |""".stripMargin

  private def metrics_snapshot(evt: MetricsSnapshot): String = {
    val idx = Labelled(evt.index).labelledText
    val took = Labelled(evt.took).labelledText

    s"""|${service_event(evt)}
        |  $idx, $took
        |${new SnapshotPolyglot(evt.snapshot).toYaml}
        |""".stripMargin
  }

  private def reported_event(evt: ReportedEvent): String = {
    val correlation = Labelled(evt.correlation).labelledText
    val domain = Labelled(evt.domain).labelledText
    val message = evt.logRecord.message.value.spaces2

    show"""|${service_event(evt)}
           |  $domain, $correlation
           |${evt.logRecord.stackTrace.fold(message) { st =>
            s"""|$message
                |${Labelled(st).labelledText}""".stripMargin
          }}
           |""".stripMargin
  }

  def apply[F[_]: Applicative]: Translator[F, String] =
    Translator
      .empty[F, String]
      .withServiceStart(service_start)
      .withServiceStop(service_stop)
      .withServicePanic(service_panic)
      .withMetricsSnapshot(metrics_snapshot)
      .withReportedEvent(reported_event)
}
