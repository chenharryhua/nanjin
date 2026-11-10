package com.github.chenharryhua.nanjin.guard.translator
import cats.data.ContT
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.order.given
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

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}
import java.util.Locale

/** Number formatter shared by the presentation renderers (`SnapshotPolyglot`, `JsonView`).
  *
  * `"#,##0.###"` groups the integer part with thousands separators and keeps up to three fractional digits,
  * dropping trailing zeros. The earlier grouping-only `"#,###"` rounded every fractional value to a whole
  * number, so a meter ticking 0.4/s rendered as `0 x/s`, a histogram's 0.5 standard deviation rendered as
  * `0`, and a ratio gauge of 0.85 rendered as `1`.
  *
  * `Locale.ROOT` symbols pin the separators to `,` and `.` instead of taking them from the JVM default, so
  * rendered output does not vary by host.
  *
  * A fresh instance per call, deliberately: `DecimalFormat` is mutable and not thread-safe, and these
  * renderers run concurrently, since the dashboard and each observer translate snapshots independently of one
  * another.
  */
private def number_formatter(): DecimalFormat =
  new DecimalFormat("#,##0.###", DecimalFormatSymbols.getInstance(Locale.ROOT))

/** The severity a rendered event is reported at, used by `AnsiTextTranslator`'s title colouring and by
  * `htmlColoring`.
  *
  * Most events map straight from their kind: a start is `Info`, a panic is `Error`, and a stop takes its
  * level from the stop reason (`Successfully` -> `Good`, `ByCancellation` -> `Warn`, `ByException` ->
  * `Error`, `Maintenance` -> `Info`). A reported event carries its own level.
  *
  * A metrics snapshot is the one composite case: it takes the '''maximum''' of three independent signals, so
  * the worst of them decides the level.
  *   - any failing `healthCheck` gauge raises it to `Error`;
  *   - any non-zero risk counter raises it to `Warn`;
  *   - an '''empty''' snapshot raises it to `Warn`.
  *
  * The third is worth knowing about: a service that deliberately registers no metrics reports every periodic
  * snapshot at `Warn`, since an empty report is treated as a sign that metrics are not wired up rather than
  * as a clean bill of health.
  */
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
    case ReportedEvent(_, _, _, _, _, report)  => report.level
    case MetricsSnapshot(_, _, _, snapshot, _) =>
      val health: LogLevel =
        if retrieve.healthCheck(snapshot.gauges).forall(_._2) then LogLevel.Info else LogLevel.Error
      val risk: LogLevel =
        if retrieve.riskCounter(snapshot.counters).forall(_._2.value === 0) then LogLevel.Info
        else LogLevel.Warn
      val nonEmpty: LogLevel =
        if snapshot.nonEmpty then LogLevel.Info else LogLevel.Warn

      health.max(risk).max(nonEmpty)
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
    case _: Event.ServiceStop                       => "Stop Service"
    case _: Event.ServicePanic                      => "Service Panic"
    case Event.ReportedEvent(_, _, _, _, _, report) => report.level.productPrefix
    case _: Event.MetricsSnapshot                   => "Metrics Report"
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
