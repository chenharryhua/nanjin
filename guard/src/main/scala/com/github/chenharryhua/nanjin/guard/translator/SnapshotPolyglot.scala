package com.github.chenharryhua.nanjin.guard.translator

import cats.data.NonEmptyList
import cats.implicits.showInterpolator
import com.github.chenharryhua.nanjin.guard.config.{Category, MetricID}
import com.github.chenharryhua.nanjin.guard.event.MeasurementUnit.*
import com.github.chenharryhua.nanjin.guard.event.{MeasurementUnit, MetricSnapshot}
import io.circe.Json
import io.circe.syntax.EncoderOps
import squants.time.TimeConversions

final class SnapshotPolyglot(snapshot: MetricSnapshot) {

  private def normalize[A: Numeric](mu: MeasurementUnit, data: A): String =
    mu match {
      case unit: NJTimeUnit          => fmt.format(TimeConversions.timeToScalaDuration(unit.mUnit(data)))
      case unit: NJInformationUnit   => s"${decimal_fmt.format(unit.mUnit(data).value.toLong)} ${unit.symbol}"
      case unit: NJDataRateUnit      => s"${decimal_fmt.format(unit.mUnit(data).value.toLong)} ${unit.symbol}"
      case unit: NJDimensionlessUnit => s"${decimal_fmt.format(unit.mUnit(data).value.toLong)} ${unit.symbol}"
    }

  private def meters: List[(MetricID, NonEmptyList[(String, Json)])] =
    snapshot.meters.map { m =>
      m.metricId -> NonEmptyList.of(
        "sum" -> Json.fromString(normalize(m.meter.unit, m.meter.sum)),
        "mean_rate" -> Json.fromString(s"${normalize(m.meter.unit, m.meter.mean_rate.toHertz)}/s"),
        "m1_rate" -> Json.fromString(s"${normalize(m.meter.unit, m.meter.m1_rate.toHertz)}/s"),
        "m5_rate" -> Json.fromString(s"${normalize(m.meter.unit, m.meter.m5_rate.toHertz)}/s"),
        "m15_rate" -> Json.fromString(s"${normalize(m.meter.unit, m.meter.m15_rate.toHertz)}/s")
      )
    }

  private def timers: List[(MetricID, NonEmptyList[(String, Json)])] =
    snapshot.timers.map { t =>
      val unit = s"calls/${NJTimeUnit.SECONDS.symbol}"
      t.metricId -> NonEmptyList.of(
        "calls" -> Json.fromString(decimal_fmt.format(t.timer.calls)),
        "mean_rate" -> Json.fromString(s"${decimal_fmt.format(t.timer.mean_rate.toHertz)} $unit"),
        "m1_rate" -> Json.fromString(s"${decimal_fmt.format(t.timer.m1_rate.toHertz)} $unit"),
        "m5_rate" -> Json.fromString(s"${decimal_fmt.format(t.timer.m5_rate.toHertz)} $unit"),
        "m15_rate" -> Json.fromString(s"${decimal_fmt.format(t.timer.m15_rate.toHertz)} $unit"),
        "min" -> Json.fromString(fmt.format(t.timer.min)),
        "max" -> Json.fromString(fmt.format(t.timer.max)),
        "mean" -> Json.fromString(fmt.format(t.timer.mean)),
        "stddev" -> Json.fromString(fmt.format(t.timer.stddev)),
        "p50" -> Json.fromString(fmt.format(t.timer.p50)),
        "p75" -> Json.fromString(fmt.format(t.timer.p75)),
        "p95" -> Json.fromString(fmt.format(t.timer.p95)),
        "p98" -> Json.fromString(fmt.format(t.timer.p98)),
        "p99" -> Json.fromString(fmt.format(t.timer.p99)),
        "p999" -> Json.fromString(fmt.format(t.timer.p999))
      )
    }

  private def histograms: List[(MetricID, NonEmptyList[(String, Json)])] =
    snapshot.histograms.map { h =>
      val unit  = h.histogram.unit
      val histo = h.histogram
      h.metricId -> NonEmptyList.of(
        "updates" -> Json.fromString(decimal_fmt.format(histo.updates)),
        "min" -> Json.fromString(normalize(unit, histo.min)),
        "max" -> Json.fromString(normalize(unit, histo.max)),
        "mean" -> Json.fromString(normalize(unit, histo.mean)),
        "stddev" -> Json.fromString(normalize(unit, histo.stddev)),
        "p50" -> Json.fromString(normalize(unit, histo.p50)),
        "p75" -> Json.fromString(normalize(unit, histo.p75)),
        "p95" -> Json.fromString(normalize(unit, histo.p95)),
        "p98" -> Json.fromString(normalize(unit, histo.p98)),
        "p99" -> Json.fromString(normalize(unit, histo.p99)),
        "p999" -> Json.fromString(normalize(unit, histo.p999))
      )
    }

  private def json_list(lst: List[(MetricID, NonEmptyList[(String, Json)])]): List[(MetricID, Json)] =
    lst.map { case (id, items) =>
      id -> items.map { case (key, js) => Json.obj(key -> js) }.reduce[Json]((a, b) => b.deepMerge(a))
    }

  private def group_json(pairs: List[(MetricID, Json)]): Json =
    pairs
      .groupBy(_._1.metricLabel.measurement) // measurement group
      .toList
      .sortBy(_._1)
      .map { case (measurement, lst) =>
        val arr: List[Json] = lst
          .groupBy(_._1.metricLabel) // metric-name group
          .toList
          .sortBy(_._1.label)
          .map { case (name, items) =>
            val inner: Json =
              items
                .sortBy(_._1.metricName)
                .map { case (mId, js) =>
                  Json.obj(mId.metricName.name -> js)
                }
                .reduce((a, b) => b.deepMerge(a))

            name -> inner.asJson
          }
          .map { case (n, js) => Json.obj("digest" -> Json.fromString(n.digest), n.label -> js) }
        Json.obj(measurement -> Json.arr(arr*))
      }
      .asJson

  // for database etc
  def toVanillaJson: Json = {
    val counters   = snapshot.counters.map(c => c.metricId -> Json.fromLong(c.count))
    val gauges     = snapshot.gauges.map(g => g.metricId -> g.value)
    val meters     = snapshot.meters.map(m => m.metricId -> m.meter.asJson)
    val histograms = snapshot.histograms.map(h => h.metricId -> h.histogram.asJson)
    val timers     = snapshot.timers.map(t => t.metricId -> t.timer.asJson)
    group_json(counters ::: gauges ::: meters ::: histograms ::: timers)
  }

  // for screen display
  def toPrettyJson: Json = {
    val counters = snapshot.counters.map(c => c.metricId -> Json.fromString(decimal_fmt.format(c.count)))
    val gauges   = snapshot.gauges.map(g => g.metricId -> g.value)

    val lst: List[(MetricID, Json)] =
      counters ::: gauges ::: json_list(meters ::: histograms ::: timers)
    group_json(lst)
  }

  /** Homemade Yaml
    */

  private def counter_str: List[(MetricID, List[String])] =
    snapshot.counters
      .filter(_.count > 0)
      .map(c => c.metricId -> List(show"${c.metricId.metricName.name}: ${decimal_fmt.format(c.count)}"))

  private def gauge_str: List[(MetricID, List[String])] =
    snapshot.gauges.map { g =>
      val str: String = g.value.fold(
        jsonNull = "null",
        jsonBoolean = _.toString,
        jsonNumber = n => decimal_fmt.format(n.toDouble),
        jsonString = identity,
        jsonArray = js => show"[${js.map(_.noSpaces).mkString(", ")}]",
        jsonObject = js => js.toJson.noSpaces
      )
      g.metricId -> List(show"${g.metricId.metricName.name}: $str")
    }

  private def meter_str: List[(MetricID, List[String])] =
    snapshot.meters.map { m =>
      m.metricId -> List(
        show"sum: ${normalize(m.meter.unit, m.meter.sum)}",
        show"mean_rate: ${normalize(m.meter.unit, m.meter.mean_rate.toHertz)}/s",
        show"  m1_rate: ${normalize(m.meter.unit, m.meter.m1_rate.toHertz)}/s",
        show"  m5_rate: ${normalize(m.meter.unit, m.meter.m5_rate.toHertz)}/s",
        show" m15_rate: ${normalize(m.meter.unit, m.meter.m15_rate.toHertz)}/s"
      )
    }

  private def timer_str: List[(MetricID, List[String])] =
    snapshot.timers.map { t =>
      val unit = s"calls/${NJTimeUnit.SECONDS.symbol}"
      t.metricId -> List(
        show"calls: ${decimal_fmt.format(t.timer.calls)}",
        show"mean_rate: ${decimal_fmt.format(t.timer.mean_rate.toHertz)} $unit",
        show"  m1_rate: ${decimal_fmt.format(t.timer.m1_rate.toHertz)} $unit",
        show"  m5_rate: ${decimal_fmt.format(t.timer.m5_rate.toHertz)} $unit",
        show" m15_rate: ${decimal_fmt.format(t.timer.m15_rate.toHertz)} $unit",
        show"      min: ${fmt.format(t.timer.min)}",
        show"      max: ${fmt.format(t.timer.max)}",
        show"     mean: ${fmt.format(t.timer.mean)}",
        show"   stddev: ${fmt.format(t.timer.stddev)}",
        show"      p50: ${fmt.format(t.timer.p50)}",
        show"      p75: ${fmt.format(t.timer.p75)}",
        show"      p95: ${fmt.format(t.timer.p95)}",
        show"      p98: ${fmt.format(t.timer.p98)}",
        show"      p99: ${fmt.format(t.timer.p99)}",
        show"     p999: ${fmt.format(t.timer.p999)}"
      )
    }

  private def histogram_str: List[(MetricID, List[String])] =
    snapshot.histograms.map { h =>
      val unit  = h.histogram.unit
      val histo = h.histogram
      h.metricId -> List(
        show"updates: ${decimal_fmt.format(histo.updates)}",
        show"    min: ${normalize(unit, histo.min)}",
        show"    max: ${normalize(unit, histo.max)}",
        show"   mean: ${normalize(unit, histo.mean)}",
        show" stddev: ${normalize(unit, histo.stddev)}",
        show"    p50: ${normalize(unit, histo.p50)}",
        show"    p75: ${normalize(unit, histo.p75)}",
        show"    p95: ${normalize(unit, histo.p95)}",
        show"    p98: ${normalize(unit, histo.p98)}",
        show"    p99: ${normalize(unit, histo.p99)}",
        show"   p999: ${normalize(unit, histo.p999)}"
      )
    }

  private val space: String = " "

  private def group_yaml(pairs: List[(MetricID, List[String])]): List[String] =
    pairs
      .groupBy(_._1.metricLabel.measurement) // measurement group
      .toList
      .sortBy(_._1)
      .flatMap { case (measurement, measurements) =>
        val arr: List[String] = measurements
          .groupBy(_._1.metricLabel) // metric-name group
          .toList
          .map { case (name, items) =>
            val oldest = items.map(_._1.metricName.order).min
            (oldest, name) -> items.sortBy(_._1.metricName).flatMap { case (id, lst) =>
              @inline def others: List[String] =
                List(id.metricName.name + ":").map(space * 4 + _) ::: lst.map(space * 6 + _)
              id.category match {
                case _: Category.Gauge     => lst.map(space * 4 + _)
                case _: Category.Counter   => lst.map(space * 4 + _)
                case _: Category.Timer     => others
                case _: Category.Meter     => others
                case _: Category.Histogram => others
              }
            }
          }
          .sortBy(_._1._1)
          .flatMap { case ((_, n), items) =>
            s"${space * 2}[${n.digest}][${n.label}]:" :: items
          }
        show"- $measurement:" :: arr
      }

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
