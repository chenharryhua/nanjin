package com.github.chenharryhua.nanjin.guard.translator

import cats.data.NonEmptyList
import cats.implicits.{catsSyntaxEq, showInterpolator, toFunctorFilterOps}
import com.github.chenharryhua.nanjin.guard.config.MetricID
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.*
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, MetricSnapshot}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.StringUtils
import squants.time.{Frequency, TimeConversions}

final class SnapshotPolyglot(snapshot: MetricSnapshot) {

  private def normalize[A: Numeric](mu: MeasurementUnit, data: A): String =
    mu match {
      case unit: NJTimeUnit =>
        durationFormatter.format(TimeConversions.timeToScalaDuration(unit.mUnit(data)))
      case unit: NJInformationUnit =>
        s"${decimalFormatter.format(unit.mUnit(data).value)} ${unit.symbol}"
      case unit: NJDataRateUnit =>
        s"${decimalFormatter.format(unit.mUnit(data).value)} ${unit.symbol}"
      case unit: NJDimensionlessUnit =>
        s"${decimalFormatter.format(unit.mUnit(data).value)} ${unit.symbol}"
    }

  private def meters: List[(MetricID, NonEmptyList[(String, String)])] =
    snapshot.meters.map { m =>
      m.metricId -> NonEmptyList.of(
        "aggregate" -> normalize(m.meter.unit, m.meter.aggregate),
        "mean_rate" -> s"${normalize(m.meter.unit, m.meter.mean_rate.toHertz)}/s",
        "m1_rate" -> s"${normalize(m.meter.unit, m.meter.m1_rate.toHertz)}/s",
        "m5_rate" -> s"${normalize(m.meter.unit, m.meter.m5_rate.toHertz)}/s",
        "m15_rate" -> s"${normalize(m.meter.unit, m.meter.m15_rate.toHertz)}/s"
      )
    }

  private def call_rate(rate: Frequency): String = {
    val hertz = rate.toHertz
    if (hertz > 1)
      s"${decimalFormatter.format(hertz)} calls/${NJTimeUnit.SECONDS.symbol}"
    else if (hertz * 60 > 1)
      s"${decimalFormatter.format(hertz * 60)} calls/${NJTimeUnit.MINUTES.symbol}"
    else if (hertz * 3600 > 1)
      s"${decimalFormatter.format(hertz * 3600)} calls/${NJTimeUnit.HOURS.symbol}"
    else
      s"${decimalFormatter.format(hertz * 864000)} calls/${NJTimeUnit.DAYS.symbol}"
  }

  private def timers: List[(MetricID, NonEmptyList[(String, String)])] =
    snapshot.timers.map { t =>
      t.metricId -> NonEmptyList.of(
        "invocations" -> decimalFormatter.format(t.timer.calls),
        "mean_rate" -> call_rate(t.timer.mean_rate),
        "m1_rate" -> call_rate(t.timer.m1_rate),
        "m5_rate" -> call_rate(t.timer.m5_rate),
        "m15_rate" -> call_rate(t.timer.m15_rate),
        "min" -> durationFormatter.format(t.timer.min),
        "max" -> durationFormatter.format(t.timer.max),
        "mean" -> durationFormatter.format(t.timer.mean),
        "stddev" -> durationFormatter.format(t.timer.stddev),
        "p50" -> durationFormatter.format(t.timer.p50),
        "p75" -> durationFormatter.format(t.timer.p75),
        "p95" -> durationFormatter.format(t.timer.p95),
        "p98" -> durationFormatter.format(t.timer.p98),
        "p99" -> durationFormatter.format(t.timer.p99),
        "p999" -> durationFormatter.format(t.timer.p999)
      )
    }

  private def histograms: List[(MetricID, NonEmptyList[(String, String)])] =
    snapshot.histograms.map { h =>
      val unit = h.histogram.unit
      val histo = h.histogram
      h.metricId -> NonEmptyList.of(
        "updates" -> decimalFormatter.format(histo.updates),
        "min" -> normalize(unit, histo.min),
        "max" -> normalize(unit, histo.max),
        "mean" -> normalize(unit, histo.mean),
        "stddev" -> normalize(unit, histo.stddev),
        "p50" -> normalize(unit, histo.p50),
        "p75" -> normalize(unit, histo.p75),
        "p95" -> normalize(unit, histo.p95),
        "p98" -> normalize(unit, histo.p98),
        "p99" -> normalize(unit, histo.p99),
        "p999" -> normalize(unit, histo.p999)
      )
    }

  private def json_list(lst: List[(MetricID, NonEmptyList[(String, String)])]): List[(MetricID, Json)] =
    lst.map { case (id, items) =>
      id -> items.map { case (key, js) => Json.obj(key -> Json.fromString(js)) }.reduce[Json]((a, b) =>
        b.deepMerge(a))
    }

  private def group_json(pairs: List[(MetricID, Json)]): Json =
    pairs
      .groupBy(_._1.metricLabel.domain) // domain group
      .toList
      .sortBy(_._1.value) // sort by domain name.
      .map { case (domain, lst) =>
        val arr: List[Json] = lst
          .groupBy(_._1.metricLabel) // metric-name group
          .toList
          .map { case (label, items) =>
            val age = items.map(_._1.metricName.age).min
            val inner: Json =
              items
                .sortBy(_._1.metricName)
                .map { case (mId, js) => Json.obj(mId.metricName.name -> js) }
                .reduce((a, b) => b.deepMerge(a))

            age -> Json.obj(label.label -> inner.asJson)
          }
          .sortBy(_._1)
          .map(_._2)
        val age = lst.map(_._1.metricName.age).min
        age -> Json.obj(domain.value -> Json.arr(arr*))
      }
      .sortBy(_._1)
      .map(_._2)
      .asJson

  // for database etc
  def toVanillaJson: Json = {
    val counters = snapshot.counters.map(c => c.metricId -> Json.fromLong(c.count))
    val gauges = snapshot.gauges.map(g => g.metricId -> g.value)
    val meters = snapshot.meters.map(m => m.metricId -> m.meter.asJson)
    val histograms = snapshot.histograms.map(h => h.metricId -> h.histogram.asJson)
    val timers = snapshot.timers.map(t => t.metricId -> t.timer.asJson)
    group_json(counters ::: gauges ::: meters ::: histograms ::: timers)
  }

  // for screen display
  def toPrettyJson: Json = {
    val counters: List[(MetricID, Json)] =
      snapshot.counters.map(c => c.metricId -> Json.fromLong(c.count))
    val gauges: List[(MetricID, Json)] =
      snapshot.gauges.mapFilter(g => if (g.value === Json.Null) None else Some(g.metricId -> g.value))

    val lst: List[(MetricID, Json)] =
      counters ::: gauges ::: json_list(meters ::: histograms ::: timers)
    group_json(lst)
  }

  /** Homemade Yaml
    */

  private def counter_str: List[(MetricID, List[String])] =
    snapshot.counters
      .filter(_.count > 0)
      .map(c => c.metricId -> List(show"${c.metricId.metricName.name}: ${decimalFormatter.format(c.count)}"))

  private val space: String = StringUtils.SPACE

  private def gauge_str: List[(MetricID, List[String])] =
    snapshot.gauges.mapFilter { g =>
      val content = JsonF.yml(g.metricId.metricName.name, g.value)
      if (content.isEmpty) None
      else
        Some(g.metricId -> content)
    }

  private def padded(kv: (String, String)): String =
    s"${space * 2}${StringUtils.leftPad(kv._1, 11)}: ${kv._2}"

  private def named(id: MetricID, data: NonEmptyList[String]): List[String] =
    s"${id.metricName.name}:" :: data.toList

  private def meter_str: List[(MetricID, List[String])] =
    meters.map { case (id, data) => id -> named(id, data.map(padded)) }

  private def timer_str: List[(MetricID, List[String])] =
    timers.map { case (id, data) => id -> named(id, data.map(padded)) }

  private def histogram_str: List[(MetricID, List[String])] =
    histograms.map { case (id, data) => id -> named(id, data.map(padded)) }

  private def group_yaml(pairs: List[(MetricID, List[String])]): List[String] =
    pairs
      .groupBy(_._1.metricLabel.domain) // domain group
      .toList
      .sortBy(_._1.value)
      .map { case (domain, domains) =>
        val arr: List[String] = domains
          .groupBy(_._1.metricLabel) // metric-name group
          .toList
          .map { case (name, items) =>
            val age = items.map(_._1.metricName.age).min
            (age, name) -> items.sortBy(_._1.metricName).flatMap(_._2.map(space * 4 + _))
          }
          .sortBy(_._1._1)
          .flatMap { case ((_, n), items) =>
            s"${space * 2}- ${n.label}:" :: items
          }
        val age = domains.map(_._1.metricName.age).min
        (age, show"[$domain]:" :: arr)
      }
      .sortBy(_._1)
      .flatMap(_._2)

  // for screen display
  def toYaml: String = {
    val lst: List[(MetricID, List[String])] =
      counter_str ::: gauge_str ::: meter_str ::: histogram_str ::: timer_str
    group_yaml(lst).mkString("\n")
  }

  // for slack
  def counterYaml: Option[String] =
    group_yaml(gauge_str ::: counter_str) match {
      case Nil => None
      case lst => Some(lst.mkString("\n"))
    }
}
