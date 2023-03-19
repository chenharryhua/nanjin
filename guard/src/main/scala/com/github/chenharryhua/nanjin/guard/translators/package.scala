package com.github.chenharryhua.nanjin.guard

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionEvent, InstantEvent}
import com.github.chenharryhua.nanjin.guard.event.{showStandardUnit, MetricSnapshot}
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
    s"${ae.title} ${ae.name.show}"

  final private[translators] def instantEventTitle(ie: InstantEvent): String =
    s"${ie.title} ${ie.name.show}"

  final private[translators] def showSnapshot(sp: ServiceParams, ss: MetricSnapshot): String = {
    val counters = ss.counters.map(c => s"  ${c.id.show} = ${c.count}")
    val gauges   = ss.gauges.map(g => s"  ${g.id.show} = ${g.value.noSpaces}")
    val unit     = sp.metricParams.rateUnitName
    val convert  = sp.metricParams.rateConversion _
    val timers = ss.timers.map { t =>
      f"""|  ${t.id.show}
          |             count = ${t.data.count}%d
          |         mean rate = ${convert(t.data.mean_rate.toHertz)}%2.2f calls/$unit
          |     1-minute rate = ${convert(t.data.m1_rate.toHertz)}%2.2f calls/$unit
          |     5-minute rate = ${convert(t.data.m5_rate.toHertz)}%2.2f calls/$unit
          |    15-minute rate = ${convert(t.data.m15_rate.toHertz)}%2.2f calls/$unit
          |               min = ${fmt.format(t.data.min)}
          |               max = ${fmt.format(t.data.max)}
          |              mean = ${fmt.format(t.data.mean)}
          |            stddev = ${fmt.format(t.data.stddev)}
          |            median = ${fmt.format(t.data.p50)}
          |              75%% <= ${fmt.format(t.data.p75)}
          |              95%% <= ${fmt.format(t.data.p95)}
          |              98%% <= ${fmt.format(t.data.p98)}
          |              99%% <= ${fmt.format(t.data.p99)}
          |            99.9%% <= ${fmt.format(t.data.p999)}""".stripMargin
    }

    val meters = ss.meters.map { m =>
      val meas = m.data.unit.show
      f"""|  ${m.id.show}
          |             count = ${m.data.count}%d
          |         mean rate = ${convert(m.data.mean_rate.toHertz)}%2.2f $meas/$unit
          |     1-minute rate = ${convert(m.data.m1_rate.toHertz)}%2.2f $meas/$unit
          |     5-minute rate = ${convert(m.data.m5_rate.toHertz)}%2.2f $meas/$unit
          |    15-minute rate = ${convert(m.data.m15_rate.toHertz)}%2.2f $meas/$unit""".stripMargin
    }

    val histograms = ss.histograms.map { h =>
      val unit = h.data.unit.show
      f"""|  ${h.id.show}
          |             count = ${h.data.count}%d
          |               min = ${h.data.min}%d $unit
          |               max = ${h.data.max}%d $unit
          |              mean = ${h.data.mean}%2.2f $unit
          |            stddev = ${h.data.stddev}%2.2f $unit
          |            median = ${h.data.p50}%2.2f $unit
          |              75%% <= ${h.data.p75}%2.2f $unit
          |              95%% <= ${h.data.p95}%2.2f $unit
          |              98%% <= ${h.data.p98}%2.2f $unit
          |              99%% <= ${h.data.p99}%2.2f $unit
          |            99.9%% <= ${h.data.p999}%2.2f $unit""".stripMargin
    }

    (gauges ::: counters ::: meters ::: histograms ::: timers).mkString("\n")
  }
}
