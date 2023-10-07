package com.github.chenharryhua.nanjin.guard.translators

import cats.data.NonEmptyList
import com.github.chenharryhua.nanjin.common.optics.jsonPlated
import com.github.chenharryhua.nanjin.guard.config.MetricID
import com.github.chenharryhua.nanjin.guard.event.MetricSnapshot
import io.circe.syntax.EncoderOps
import io.circe.{Json, Printer}
import monocle.function.Plated

import java.text.DecimalFormat

final class SnapshotPolyglot(snapshot: MetricSnapshot) {
  private val decFmt: DecimalFormat = new DecimalFormat("#,###")
  private val prettyNumber: Json => Json = Plated.transform[Json] { js =>
    js.asNumber match {
      case Some(value) => Json.fromString(decFmt.format(value.toDouble))
      case None        => js
    }
  }

  private def counters: List[(MetricID, Json)] =
    snapshot.counters.map(c => c.metricId -> Json.fromLong(c.count))
  private def gauges: List[(MetricID, Json)] =
    snapshot.gauges.map(g => g.metricId -> g.value)

  private def meters: List[(MetricID, NonEmptyList[(String, Json)])] = snapshot.meters.map { m =>
    val unit = s"${m.meter.unit.symbol}/second"
    m.metricId -> NonEmptyList.of(
      "count" -> Json.fromLong(m.meter.count),
      "mean_rate" -> Json.fromString(s"${decFmt.format(m.meter.mean_rate.toHertz)} $unit"),
      "m1_rate  " -> Json.fromString(s"${decFmt.format(m.meter.m1_rate.toHertz)} $unit"),
      "m5_rate  " -> Json.fromString(s"${decFmt.format(m.meter.m5_rate.toHertz)} $unit"),
      "m15_rate " -> Json.fromString(s"${decFmt.format(m.meter.m15_rate.toHertz)} $unit")
    )
  }
  private def timers: List[(MetricID, NonEmptyList[(String, Json)])] = snapshot.timers.map { t =>
    val unit = "calls/second"
    t.metricId -> NonEmptyList.of(
      "count" -> Json.fromLong(t.timer.count),
      "mean_rate" -> Json.fromString(s"${decFmt.format(t.timer.mean_rate.toHertz)} $unit"),
      "m1_rate  " -> Json.fromString(s"${decFmt.format(t.timer.m1_rate.toHertz)} $unit"),
      "m5_rate  " -> Json.fromString(s"${decFmt.format(t.timer.m5_rate.toHertz)} $unit"),
      "m15_rate " -> Json.fromString(s"${decFmt.format(t.timer.m15_rate.toHertz)} $unit"),
      "min   " -> Json.fromString(fmt.format(t.timer.min)),
      "max   " -> Json.fromString(fmt.format(t.timer.max)),
      "mean  " -> Json.fromString(fmt.format(t.timer.mean)),
      "stddev" -> Json.fromString(fmt.format(t.timer.stddev)),
      "p50   " -> Json.fromString(fmt.format(t.timer.p50)),
      "p75   " -> Json.fromString(fmt.format(t.timer.p75)),
      "p95   " -> Json.fromString(fmt.format(t.timer.p95)),
      "p98   " -> Json.fromString(fmt.format(t.timer.p98)),
      "p99   " -> Json.fromString(fmt.format(t.timer.p99)),
      "p999  " -> Json.fromString(fmt.format(t.timer.p999))
    )
  }

  private def histograms: List[(MetricID, NonEmptyList[(String, Json)])] = snapshot.histograms.map { h =>
    val unit = h.histogram.unit.symbol
    h.metricId -> NonEmptyList.of(
      "count" -> Json.fromLong(h.histogram.count),
      "min   " -> Json.fromString(s"${decFmt.format(h.histogram.min)} $unit"),
      "max   " -> Json.fromString(s"${decFmt.format(h.histogram.max)} $unit"),
      "mean  " -> Json.fromString(s"${decFmt.format(h.histogram.mean)} $unit"),
      "stddev" -> Json.fromString(s"${decFmt.format(h.histogram.stddev)} $unit"),
      "p50   " -> Json.fromString(s"${decFmt.format(h.histogram.p50)} $unit"),
      "p75   " -> Json.fromString(s"${decFmt.format(h.histogram.p75)} $unit"),
      "p95   " -> Json.fromString(s"${decFmt.format(h.histogram.p95)} $unit"),
      "p98   " -> Json.fromString(s"${decFmt.format(h.histogram.p98)} $unit"),
      "p99   " -> Json.fromString(s"${decFmt.format(h.histogram.p99)} $unit"),
      "p999  " -> Json.fromString(s"${decFmt.format(h.histogram.p999)} $unit")
    )
  }

  private def jsonList(lst: List[(MetricID, NonEmptyList[(String, Json)])]): List[(MetricID, Json)] =
    lst.map { case (id, items) =>
      id -> items.map { case (key, js) => Json.obj(key -> js) }.reduce[Json]((a, b) => b.deepMerge(a))
    }

  private def trimKey(
    lst: List[(MetricID, NonEmptyList[(String, Json)])]): List[(MetricID, NonEmptyList[(String, Json)])] =
    lst.map { case (id, items) => id -> items.map { case (key, js) => key.trim -> js } }

  private def groupingJson(pairs: List[(MetricID, Json)]): Json =
    pairs
      .groupBy(_._1.metricName.measurement) // measurement group
      .map { case (measurement, lst) =>
        val arr = lst
          .groupBy(_._1.metricName) // metric-name group
          .map { case (name, js) =>
            val inner =
              js.map { case (mId, j) =>
                Json.obj("digest" -> Json.fromString(mId.metricName.digest), mId.category.name -> j)
              }.reduce((a, b) => b.deepMerge(a))
            name -> inner
          }
          .toList
          .sortBy(_._1)
          .map { case (n, j) => Json.obj(n.value -> j) }
        measurement -> Json.arr(arr*)
      }
      .toList
      .sortBy(_._1)
      .map { case (m, j) => Json.obj(m -> j) }
      .asJson

  def toVanillaJson: Json = {
    val meters     = snapshot.meters.map(m => m.metricId -> m.meter.asJson)
    val histograms = snapshot.histograms.map(h => h.metricId -> h.histogram.asJson)
    val timers     = snapshot.timers.map(t => t.metricId -> t.timer.asJson)
    groupingJson(counters ::: gauges ::: meters ::: histograms ::: timers)
  }

  def toPrettyJson: Json = {
    val lst: List[(MetricID, Json)] =
      counters ::: gauges ::: jsonList(trimKey(meters ::: histograms ::: timers))
    prettyNumber(groupingJson(lst))
  }

  /** Homemade Yaml
    */

  @inline private val leftParen  = "(" * 7
  @inline private val rightParen = ")" * 7

  private def groupingYaml(pairs: List[(MetricID, Json)]): List[Json] =
    pairs
      .groupBy(_._1.metricName.measurement) // measurement group
      .map { case (measurement, lst) =>
        val arr = lst
          .groupBy(_._1.metricName) // metric-name group
          .map { case (name, js) =>
            val inner =
              js.map { case (mId, j) => Json.obj(mId.category.name -> j) }.reduce((a, b) => b.deepMerge(a))
            name -> inner
          }
          .toList
          .sortBy(_._1)
          .map { case (n, j) =>
            val key = s"$leftParen${n.digest}$rightParen$leftParen${n.value}$rightParen"
            Json.obj(key -> j)
          }
        s"- $measurement" -> Json.arr(arr*)
      }
      .toList
      .sortBy(_._1)
      .map { case (m, j) => Json.obj(m -> j) }

  private def yamlGen(json: Json): String =
    Printer(
      dropNullValues = true,
      indent = "  ",
      sortKeys = false,
      lbraceRight = "\n",
      objectCommaRight = "\n",
      colonRight = " ")
      .print(json)
      .replaceAll("""[{}\[\]"]""", "")
      .replace(leftParen, "[")
      .replace(rightParen, "]")
      .linesIterator
      .drop(1) // drop empty line
      .map(line => if (line.endsWith(",")) line.dropRight(1) else line)
      .mkString("\n")

  def toYaml: String = {
    val lst: List[(MetricID, Json)] = counters ::: gauges ::: jsonList(meters ::: histograms ::: timers)
    yamlGen(prettyNumber(groupingYaml(lst).asJson))
  }

  def counterYaml: Option[String] =
    groupingYaml(
      snapshot.counters
        .filter(_.count > 0)
        .map(c => c.metricId -> Json.fromString(decFmt.format(c.count)))) match {
      case Nil => None
      case lst => Some(yamlGen(lst.asJson))
    }
}
