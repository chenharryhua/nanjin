package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionEvent, InstantEvent}
import org.apache.commons.lang3.StringUtils
import org.typelevel.cats.time.instances.localdatetime.localdatetimeInstances
import org.typelevel.cats.time.instances.localtime.localtimeInstances

import java.time.temporal.ChronoUnit
import java.time.{Duration, ZonedDateTime}

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

  final private[translators] def actionTitle(ae: ActionEvent): String =
    s"${ae.title} ${ae.digested.show}"

  final private[translators] def instantEventTitle(ie: InstantEvent): String =
    s"${ie.title} ${ie.digested.show}"

  final private[translators] def showSnapshot(sp: ServiceParams, ss: MetricSnapshot): String = {
    val counters = ss.counters.map(c => s"  ${c.digested.show}.${c.category} = ${c.count}")
    val gauges   = ss.gauges.map(g => s"  ${g.digested.show}.gauge = ${g.value}")
    val unit     = sp.metricParams.rateUnitName
    val convert  = sp.metricParams.rateConversion _
    val timers = ss.timers.map { t =>
      f"""|  ${t.digested.show}.timer
          |             count = ${t.count}%d
          |         mean rate = ${convert(t.mean_rate)}%2.2f calls/$unit
          |     1-minute rate = ${convert(t.m1_rate)}%2.2f calls/$unit
          |     5-minute rate = ${convert(t.m5_rate)}%2.2f calls/$unit
          |    15-minute rate = ${convert(t.m15_rate)}%2.2f calls/$unit
          |               min = ${fmt.format(t.min)}
          |               max = ${fmt.format(t.max)}
          |              mean = ${fmt.format(t.mean)}
          |            stddev = ${fmt.format(t.stddev)}
          |            median = ${fmt.format(t.p50)}
          |              75%% <= ${fmt.format(t.p75)}
          |              95%% <= ${fmt.format(t.p95)}
          |              98%% <= ${fmt.format(t.p98)}
          |              99%% <= ${fmt.format(t.p99)}
          |            99.9%% <= ${fmt.format(t.p999)}""".stripMargin
    }

    val meters = ss.meters.map { m =>
      f"""|  ${m.digested.show}.meter
          |             count = ${m.count}%d
          |         mean rate = ${convert(m.mean_rate)}%2.2f events/$unit
          |     1-minute rate = ${convert(m.m1_rate)}%2.2f events/$unit
          |     5-minute rate = ${convert(m.m5_rate)}%2.2f events/$unit
          |    15-minute rate = ${convert(m.m15_rate)}%2.2f events/$unit""".stripMargin
    }

    val histograms = ss.histograms.map { h =>
      val unit = h.unitOfMeasure
      f"""|  ${h.digested.show}.histogram
          |             count = ${h.count}%d
          |               min = ${h.min}%d $unit
          |               max = ${h.max}%d $unit
          |              mean = ${h.mean}%2.2f $unit
          |            stddev = ${h.stddev}%2.2f $unit
          |            median = ${h.p50}%2.2f $unit
          |              75%% <= ${h.p75}%2.2f $unit
          |              95%% <= ${h.p95}%2.2f $unit
          |              98%% <= ${h.p98}%2.2f $unit
          |              99%% <= ${h.p99}%2.2f $unit
          |            99.9%% <= ${h.p999}%2.2f $unit""".stripMargin
    }

    (gauges ::: counters ::: meters ::: histograms ::: timers).mkString("\n")
  }
}
