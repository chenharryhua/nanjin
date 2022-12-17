package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, NJEvent}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionEvent, InstantEvent, MetricEvent}
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.{Duration, ZonedDateTime}
import java.time.temporal.ChronoUnit

package object translators {

  // slack not allow message larger than 3000 chars
  // https://api.slack.com/reference/surfaces/formatting
  final private[translators] val MessageSizeLimits: Int = 2500

  final private[translators] def abbreviate(msg: String): String =
    StringUtils.abbreviate(msg, MessageSizeLimits)

  final private[translators] def hostServiceSection(sp: ServiceParams): JuxtaposeSection = {
    val sn: String =
      sp.homePage.fold(sp.serviceName.value)(hp => s"<${hp.value}|${sp.serviceName.value}>")
    JuxtaposeSection(TextField("Service", sn), TextField("Host", sp.taskParams.hostName.value))
  }

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

  final private[translators] def metricTitle(me: MetricEvent): String =
    me match {
      case NJEvent.MetricReport(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc           => "Adhoc Metric Report"
          case MetricIndex.Periodic(index) => s"Metric Report(index=$index)"
        }
      case NJEvent.MetricReset(index, _, _, _) =>
        index match {
          case MetricIndex.Adhoc           => "Adhoc Metric Reset"
          case MetricIndex.Periodic(index) => s"Metric Reset(index=$index)"
        }
    }

  final private[translators] def actionTitle(ae: ActionEvent): String =
    ae.title + " " + ae.digested.metricRepr

  final private[translators] def instantEventTitle(ie: InstantEvent): String =
    ie.title + " " + ie.digested.metricRepr

}
