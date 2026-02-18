package com.github.chenharryhua.nanjin.guard.translator

import cats.syntax.eq.catsSyntaxEq
import cats.syntax.show.toShow
import com.github.chenharryhua.nanjin.guard.event.Event.ServicePanic
import com.github.chenharryhua.nanjin.guard.event.{Event, MetricSnapshot}
import org.typelevel.cats.time.instances.{localdatetime, localtime}

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

object textHelper extends localtime with localdatetime {
  def yamlMetrics(ss: MetricSnapshot): String =
    new SnapshotPolyglot(ss).toYaml

  def eventTitle(evt: Event): String =
    evt match {
      case ss: Event.ServiceStart =>
        if (ss.tick.index === 0) "Start Service" else "Restart Service"
      case _: Event.ServiceStop    => "Service Stopped"
      case _: Event.ServicePanic   => "Service Panic"
      case _: Event.ServiceMessage => "Service Message"
      case _: Event.MetricsReport  => "Metrics Report"
      case _: Event.MetricsReset   => "Metrics Reset"
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
    val (time, dur) = localTime_duration(evt.timestamp.value, evt.tick.zoned(_.conclude))
    s"Restart was scheduled at $time, in $dur."
  }
}
