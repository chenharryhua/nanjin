package com.github.chenharryhua.nanjin.guard.translator

import cats.Eval
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.ServicePanic
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricIndex, MetricSnapshot}
import org.typelevel.cats.time.instances.{localdatetime, localtime}

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

object textHelper extends localtime with localdatetime {
  def yamlMetrics(ss: MetricSnapshot): String =
    new SnapshotPolyglot(ss).toYaml

  def uptimeText(evt: Event): String = durationFormatter.format(evt.upTime)

  def tookText(dur: Duration): String = durationFormatter.format(dur)

  def hostText(sp: ServiceParams): String =
    sp.emberServerParams match {
      case Some(esp) => s"${sp.hostName.value}:${esp.port}"
      case None      => sp.hostName.value
    }

  def metricIndexText(index: MetricIndex): String =
    index match {
      case MetricIndex.Adhoc(_)       => "Adhoc"
      case MetricIndex.Periodic(tick) => show"${tick.index}"
    }

  def eventTitle(evt: Event): String =
    evt match {
      case ss: Event.ServiceStart =>
        if (ss.tick.index === 0) "Start Service" else "Restart Service"
      case _: Event.ServiceStop    => "Service Stopped"
      case _: Event.ServicePanic   => "Service Panic"
      case _: Event.ServiceMessage => "Service Message"
      case _: Event.MetricReport   => "Metric Report"
      case _: Event.MetricReset    => "Metric Reset"
    }

  private def localTime_duration(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, durationFormatter.format(duration))
  }

  def panicText(evt: ServicePanic): String = {
    val (time, dur) = localTime_duration(evt.timestamp, evt.tick.zonedWakeup)
    s"Restart was scheduled at $time, in $dur."
  }

  def consoleColor(colorScheme: ColorScheme): Eval[String] =
    colorScheme match {
      case ColorScheme.GoodColor  => Eval.now(Console.GREEN + "SUCCESS" + Console.RESET)
      case ColorScheme.InfoColor  => Eval.now(Console.CYAN + "INFO" + Console.RESET)
      case ColorScheme.WarnColor  => Eval.now(Console.YELLOW + "WARN" + Console.RESET)
      case ColorScheme.ErrorColor => Eval.now(Console.RED + "ERROR" + Console.RESET)
    }
}
