package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.NJEvent.ActionRetry
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

package object translators {

  final private def toOrdinalWords(n: Int): String = {
    val w =
      if (n % 100 / 10 == 1) "th"
      else {
        n % 10 match {
          case 1 => "st"
          case 2 => "nd"
          case 3 => "rd"
          case _ => "th"
        }
      }
    s"$n$w"
  }

  // NumberFormat and DurationFormatter are thread safe
  final private[translators] val fmt: DurationFormatter = DurationFormatter.defaultFormatter

  final private[translators] def localTimeAndDurationStr(
    start: ZonedDateTime,
    end: ZonedDateTime): (String, String) = {
    val duration = Duration.between(start, end)
    val localTime: String =
      if (duration.minus(Duration.ofHours(24)).isNegative)
        end.truncatedTo(ChronoUnit.SECONDS).toLocalTime.show
      else
        end.truncatedTo(ChronoUnit.SECONDS).toLocalDateTime.show

    (localTime, fmt.format(duration))
  }

  final private[translators] def eventTitle(evt: NJEvent): String = {
    def name(mn: MetricName): String = s"[${mn.digest}][${mn.value}]"
    evt match {
      case NJEvent.ActionStart(ap, _, _)          => s"Start Action ${name(ap.metricId.metricName)}"
      case NJEvent.ActionRetry(ap, _, _, _, _, _) => s"Action Retrying ${name(ap.metricId.metricName)}"
      case NJEvent.ActionFail(ap, _, _, _, _)     => s"Action Failed ${name(ap.metricId.metricName)}"
      case NJEvent.ActionComplete(ap, _, _, _)    => s"Action Completed ${name(ap.metricId.metricName)}"

      case NJEvent.ServiceAlert(metricName, _, _, al, _) => s"Alert ${al.productPrefix} ${name(metricName)}"

      case _: NJEvent.ServiceStart => "(Re)Start Service"
      case _: NJEvent.ServiceStop  => "Service Stopped"
      case _: NJEvent.ServicePanic => "Service Panic"

      case NJEvent.MetricReport(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc         => "Adhoc Metric Report"
          case MetricIndex.Periodic(idx) => s"Metric Report(index=$idx)"
        }
      case NJEvent.MetricReset(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc         => "Adhoc Metric Reset"
          case MetricIndex.Periodic(idx) => s"Metric Reset(index=$idx)"
        }
    }
  }

  private[translators] def retryText(evt: ActionRetry): String = {
    val resumeTime = evt.timestamp.plusNanos(evt.delay.toNanos)
    val next       = fmt.format(Duration.between(evt.timestamp, resumeTime))
    val localTs    = resumeTime.toLocalTime.truncatedTo(ChronoUnit.SECONDS)
    s"*${toOrdinalWords(evt.retriesSoFar + 1)}* retry at $localTs, in $next"
  }
}
