package com.github.chenharryhua.nanjin.guard.translators

import com.github.chenharryhua.nanjin.guard.config.{MetricID, MetricName, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, Snapshot}
import io.circe.optics.JsonOptics.*
import io.circe.syntax.EncoderOps
import io.circe.{Json, Printer}
import monocle.function.Plated

import java.text.DecimalFormat

final class SnapshotJson(snapshot: MetricSnapshot) {
  private val decFmt: DecimalFormat = new DecimalFormat("#,###.##")
  private val prettyNumber: Json => Json = Plated.transform[Json] { js =>
    js.asNumber match {
      case Some(value) => Json.fromString(decFmt.format(value.toDouble))
      case None        => js
    }
  }
  // for db
  private def grouping1(f: Snapshot => (MetricID, Json)): Json =
    (snapshot.gauges.map(f) :::
      snapshot.counters.map(f) :::
      snapshot.timers.map(f) :::
      snapshot.meters.map(f) :::
      snapshot.histograms.map(f))
      .groupBy(_._1.metricName.measurement) // measurement group
      .toList
      .flatMap { case (_, lst: List[(MetricID, Json)]) =>
        lst
          .groupBy(_._1.metricName) // metric-name group
          .map { case (name: MetricName, js: List[(MetricID, Json)]) =>
            js.map { case (mId, j) => Json.obj(mId.category.name -> j) }.foldLeft(
              Json.obj(
                "name" -> Json.fromString(name.value),
                "digest" -> Json.fromString(name.digest),
                "measurement" -> Json.fromString(name.measurement)
              ))((a, b) => b.deepMerge(a))
          }
          .toList
      }
      .asJson

  def toVanillaJson: Json =
    grouping1 {
      case Snapshot.Counter(id, count)  => id -> Json.fromLong(count)
      case Snapshot.Gauge(id, value)    => id -> value
      case Snapshot.Meter(id, data)     => id -> data.asJson
      case Snapshot.Timer(id, data)     => id -> data.asJson
      case Snapshot.Histogram(id, data) => id -> data.asJson
    }

  /** * Pretty Json
    */
  // for std-out etc
  private def grouping2(f: Snapshot => (MetricID, Json)): Json =
    (snapshot.gauges.map(f) :::
      snapshot.counters.map(f) :::
      snapshot.timers.map(f) :::
      snapshot.meters.map(f) :::
      snapshot.histograms.map(f))
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

  def toPrettyJson(mp: MetricParams): Json = {
    val rateUnit                   = mp.rateUnitName
    def convert(d: Double): String = decFmt.format(mp.rateConversion(d))
    val json = grouping2 {
      case Snapshot.Counter(id, count) => id -> Json.fromLong(count)
      case Snapshot.Gauge(id, value)   => id -> value
      case Snapshot.Meter(id, data) =>
        val unit = data.unitShow
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(s"${convert(data.mean_rate.toHertz)} $unit/$rateUnit"),
          "m1_rate" -> Json.fromString(s"${convert(data.m1_rate.toHertz)} $unit/$rateUnit"),
          "m5_rate" -> Json.fromString(s"${convert(data.m5_rate.toHertz)} $unit/$rateUnit"),
          "m15_rate" -> Json.fromString(s"${convert(data.m15_rate.toHertz)} $unit/$rateUnit")
        )
      case Snapshot.Timer(id, data) =>
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(s"${convert(data.mean_rate.toHertz)} calls/$rateUnit"),
          "m1_rate" -> Json.fromString(s"${convert(data.m1_rate.toHertz)} calls/$rateUnit"),
          "m5_rate" -> Json.fromString(s"${convert(data.m5_rate.toHertz)} calls/$rateUnit"),
          "m15_rate" -> Json.fromString(s"${convert(data.m15_rate.toHertz)} calls/$rateUnit"),
          "min" -> Json.fromString(fmt.format(data.min)),
          "max" -> Json.fromString(fmt.format(data.max)),
          "mean" -> Json.fromString(fmt.format(data.mean)),
          "stddev" -> Json.fromString(fmt.format(data.stddev)),
          "p50" -> Json.fromString(fmt.format(data.p50)),
          "p75" -> Json.fromString(fmt.format(data.p75)),
          "p95" -> Json.fromString(fmt.format(data.p95)),
          "p98" -> Json.fromString(fmt.format(data.p98)),
          "p99" -> Json.fromString(fmt.format(data.p99)),
          "p999" -> Json.fromString(fmt.format(data.p999))
        )

      case Snapshot.Histogram(id, data) =>
        val unit = data.unitShow
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "min" -> Json.fromString(s"${decFmt.format(data.min)} $unit"),
          "max" -> Json.fromString(s"${decFmt.format(data.max)} $unit"),
          "mean" -> Json.fromString(s"${decFmt.format(data.mean)} $unit"),
          "stddev" -> Json.fromString(s"${decFmt.format(data.stddev)} $unit"),
          "p50" -> Json.fromString(s"${decFmt.format(data.p50)} $unit"),
          "p75" -> Json.fromString(s"${decFmt.format(data.p75)} $unit"),
          "p95" -> Json.fromString(s"${decFmt.format(data.p95)} $unit"),
          "p98" -> Json.fromString(s"${decFmt.format(data.p98)} $unit"),
          "p99" -> Json.fromString(s"${decFmt.format(data.p99)} $unit"),
          "p999" -> Json.fromString(s"${decFmt.format(data.p999)} $unit")
        )
    }
    prettyNumber(json)
  }

  /** Homemade Yaml
    */

  @inline private val leftParen  = "(" * 7
  @inline private val rightParen = ")" * 7

  private def grouping3(f: Snapshot => (MetricID, Json)): Json =
    (snapshot.gauges.map(f) :::
      snapshot.counters.map(f) :::
      snapshot.timers.map(f) :::
      snapshot.meters.map(f) :::
      snapshot.histograms.map(f))
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
      .asJson

  private val printer: Printer = Printer(
    dropNullValues = true,
    indent = "  ",
    sortKeys = false,
    lbraceRight = "\n",
    objectCommaRight = "\n",
    colonRight = " ")

  def toYaml(mp: MetricParams): String = {
    val rateUnit: String           = mp.rateUnitName
    def convert(d: Double): String = decFmt.format(mp.rateConversion(d))
    val json = grouping3 {
      case Snapshot.Counter(id, count) => id -> Json.fromLong(count)
      case Snapshot.Gauge(id, value)   => id -> value
      case Snapshot.Meter(id, data) =>
        val unit = data.unitShow
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(s"${convert(data.mean_rate.toHertz)} $unit/$rateUnit"),
          "m1_rate  " -> Json.fromString(s"${convert(data.m1_rate.toHertz)} $unit/$rateUnit"),
          "m5_rate  " -> Json.fromString(s"${convert(data.m5_rate.toHertz)} $unit/$rateUnit"),
          "m15_rate " -> Json.fromString(s"${convert(data.m15_rate.toHertz)} $unit/$rateUnit")
        )
      case Snapshot.Timer(id, data) =>
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(s"${convert(data.mean_rate.toHertz)} calls/$rateUnit"),
          "m1_rate  " -> Json.fromString(s"${convert(data.m1_rate.toHertz)} calls/$rateUnit"),
          "m5_rate  " -> Json.fromString(s"${convert(data.m5_rate.toHertz)} calls/$rateUnit"),
          "m15_rate " -> Json.fromString(s"${convert(data.m15_rate.toHertz)} calls/$rateUnit"),
          "min   " -> Json.fromString(fmt.format(data.min)),
          "max   " -> Json.fromString(fmt.format(data.max)),
          "mean  " -> Json.fromString(fmt.format(data.mean)),
          "stddev" -> Json.fromString(fmt.format(data.stddev)),
          "p50   " -> Json.fromString(fmt.format(data.p50)),
          "p75   " -> Json.fromString(fmt.format(data.p75)),
          "p95   " -> Json.fromString(fmt.format(data.p95)),
          "p98   " -> Json.fromString(fmt.format(data.p98)),
          "p99   " -> Json.fromString(fmt.format(data.p99)),
          "p999  " -> Json.fromString(fmt.format(data.p999))
        )

      case Snapshot.Histogram(id, data) =>
        val unit = data.unitShow
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "min   " -> Json.fromString(s"${decFmt.format(data.min)} $unit"),
          "max   " -> Json.fromString(s"${decFmt.format(data.max)} $unit"),
          "mean  " -> Json.fromString(s"${decFmt.format(data.mean)} $unit"),
          "stddev" -> Json.fromString(s"${decFmt.format(data.stddev)} $unit"),
          "p50   " -> Json.fromString(s"${decFmt.format(data.p50)} $unit"),
          "p75   " -> Json.fromString(s"${decFmt.format(data.p75)} $unit"),
          "p95   " -> Json.fromString(s"${decFmt.format(data.p95)} $unit"),
          "p98   " -> Json.fromString(s"${decFmt.format(data.p98)} $unit"),
          "p99   " -> Json.fromString(s"${decFmt.format(data.p99)} $unit"),
          "p999  " -> Json.fromString(s"${decFmt.format(data.p999)} $unit")
        )
    }

    printer
      .print(prettyNumber(json))
      .replaceAll("""[{}\[\]"]""", "")
      .replace(leftParen, "[")
      .replace(rightParen, "]")
      .linesIterator
      .drop(1) // drop empty line
      .map(line => if (line.endsWith(",")) line.dropRight(1) else line)
      .mkString("\n")
  }

  def shrank: String = {
    val json = snapshot.counters
      .map(c => c.metricId -> Json.fromString(decFmt.format(c.count)))
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
      .asJson

    printer
      .print(json)
      .replaceAll("""[{}\[\]"]""", "")
      .replace(leftParen, "[")
      .replace(rightParen, "]")
      .linesIterator
      .drop(1) // drop empty line
      .map(line => if (line.endsWith(",")) line.dropRight(1) else line)
      .mkString("\n")
  }
}
