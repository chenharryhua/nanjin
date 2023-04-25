package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.MetricParams
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJEvent}
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

package object translators {

  final def toOrdinalWords(n: Int): String = {
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

  final private[translators] def eventTitle(evt: NJEvent): String =
    evt match {
      case NJEvent.ActionStart(ap, _, _)          => s"Start Action ${ap.metricId.metricName.display}"
      case NJEvent.ActionRetry(ap, _, _, _, _, _) => s"Action Retrying ${ap.metricId.metricName.display}"
      case NJEvent.ActionFail(ap, _, _, _, _)     => s"Action Failed ${ap.metricId.metricName.display}"
      case NJEvent.ActionComplete(ap, _, _, _)    => s"Action Completed ${ap.metricId.metricName.display}"

      case NJEvent.ServiceAlert(metricName, _, _, al, _) =>
        s"Alert ${StringUtils.capitalize(al.entryName)} ${metricName.display}"

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

  private[translators] def yamlSnapshot(mss: MetricSnapshot, mp: MetricParams): String =
    new SnapshotJson(mss).toYaml(mp)
}
