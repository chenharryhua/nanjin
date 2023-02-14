package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricIndex, MetricSnapshot, NJEvent}
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

  final private[translators] def showSnapshot(sp: ServiceParams, ss: MetricSnapshot): String = {
    val counters = ss.counters.map(c => s"  ${c.name} = ${c.count}")
    val gauges   = ss.gauges.map(g => s"  ${g.name} = ${g.value}")
    val rate     = s"calls/${sp.metricParams.rateTimeUnit.name().toLowerCase().dropRight(1)}"
    val timers = ss.timers.map { t =>
      f"""|  ${t.name} = ${t.count}%d
          |         mean rate = ${t.mean_rate}%2.2f $rate
          |     1-minute rate = ${t.m1_rate}%2.2f $rate
          |     5-minute rate = ${t.m5_rate}%2.2f $rate
          |    15-minute rate = ${t.m15_rate}%2.2f $rate
          |               min = ${fmt.format(t.min)}
          |               max = ${fmt.format(t.max)}
          |              mean = ${fmt.format(t.mean)}
          |            stddev = ${fmt.format(t.stddev)}
          |            median = ${fmt.format(t.median)}
          |              75%% <= ${fmt.format(t.p75)}
          |              95%% <= ${fmt.format(t.p95)}
          |              98%% <= ${fmt.format(t.p98)}
          |              99%% <= ${fmt.format(t.p99)}
          |            99.9%% <= ${fmt.format(t.p999)}""".stripMargin
    }

    val meters = ss.meters.map { m =>
      f"""|  ${m.name} = ${m.count}%d
          |         mean rate = ${m.mean_rate}%2.2f $rate
          |     1-minute rate = ${m.m1_rate}%2.2f $rate
          |     5-minute rate = ${m.m5_rate}%2.2f $rate
          |    15-minute rate = ${m.m15_rate}%2.2f $rate""".stripMargin
    }

    val histograms = ss.histograms.map { h =>
      f"""|  ${h.name} = ${h.count}%d
          |               min = ${h.min}%d
          |               max = ${h.max}%d
          |              mean = ${h.mean}%2.2f
          |            stddev = ${h.stddev}%2.2f
          |            median = ${h.median}%2.2f
          |              75%% <= ${h.p75}%2.2f
          |              95%% <= ${h.p95}%2.2f
          |              98%% <= ${h.p98}%2.2f
          |              99%% <= ${h.p99}%2.2f
          |            99.9%% <= ${h.p999}%2.2f""".stripMargin
    }

    (counters ::: gauges ::: timers ::: meters ::: histograms).mkString("\n")
  }

}
