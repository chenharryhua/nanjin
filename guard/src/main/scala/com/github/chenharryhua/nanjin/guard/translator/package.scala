package com.github.chenharryhua.nanjin.guard.translator
import cats.data.ContT
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.show.given
import cats.{Defer, Eval}
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import com.github.chenharryhua.nanjin.guard.event.Event.{
  MetricsSnapshot,
  ReportedEvent,
  ServicePanic,
  ServiceStart,
  ServiceStop
}
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.retrieve
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

def eventLogLevel[F[_]: Defer, A](evt: Event): ContT[F, A, LogLevel] =
  ContT.pure[F, A, Event](evt).map {
    case _: ServiceStart => LogLevel.Info
    case _: ServicePanic => LogLevel.Error
    case ss: ServiceStop =>
      ss.cause match
        case StopReason.Successfully   => LogLevel.Good
        case StopReason.ByCancellation => LogLevel.Warn
        case StopReason.ByException(_) => LogLevel.Error
        case StopReason.Maintenance    => LogLevel.Info
    case ReportedEvent(_, _, _, _, level, _, _) => level
    case MetricsSnapshot(_, _, _, snapshot, _)  =>
      val health = retrieve.healthCheck(snapshot.gauges).forall(_._2)
      val risk = retrieve.riskCounter(snapshot.counters).forall(_._2.value === 0)
      if health && risk then LogLevel.Info else LogLevel.Warn
  }

def htmlColoring(evt: Event): String =
  eventLogLevel[Eval, String](evt)
    .run {
      case LogLevel.Good  => Eval.now("color:darkgreen")
      case LogLevel.Info  => Eval.now("color:black")
      case LogLevel.Warn  => Eval.now("color:#b3b300")
      case LogLevel.Error => Eval.now("color:red")
      case LogLevel.Debug => Eval.now("color:#FF00FF")
    }
    .value

def eventTitle(evt: Event): String =
  evt match {
    case ss: Event.ServiceStart =>
      if (ss.tick.index === 0) "Start Service" else "Restart Service"
    case _: Event.ServiceStop                         => "Stop Service"
    case _: Event.ServicePanic                        => "Service Panic"
    case Event.ReportedEvent(_, _, _, _, level, _, _) => level.productPrefix
    case _: Event.MetricsSnapshot                     => "Metrics Report"
  }

private def localTime_duration(start: ZonedDateTime, end: ZonedDateTime): (String, String) = {
  val duration = Duration.between(start, end)
  val localTime: String =
    if (duration.minus(Duration.ofHours(24)).isNegative)
      end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
    else
      end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

  (localTime, defaultFormatter.format(duration))
}

def panicText(evt: ServicePanic): String = {
  val (time, dur) = localTime_duration(evt.timestamp.value, evt.tick.zoned(_.conclude))
  s"Restart scheduled for $time, in $dur."
}
