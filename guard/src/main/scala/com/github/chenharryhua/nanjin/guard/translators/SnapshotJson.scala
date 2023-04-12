package com.github.chenharryhua.nanjin.guard.translators

import com.github.chenharryhua.nanjin.guard.config.{MetricID, MetricParams}
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, Snapshot}
import io.circe.Json
import io.circe.syntax.EncoderOps

final class SnapshotJson(snapshot: MetricSnapshot) {
  // for db
  private def grouping1(f: Snapshot => (MetricID, Json)): Json =
    (snapshot.gauges.map(f) :::
      snapshot.counters.map(f) :::
      snapshot.timers.map(f) :::
      snapshot.meters.map(f) :::
      snapshot.histograms.map(f))
      .groupBy(_._1.metricName.measurement.value) // measurement group
      .map { case (measurement, lst) =>
        val arr = lst
          .groupBy(_._1.metricName) // metric-name group
          .map { case (name, js) =>
            val inner =
              js.map { case (mId, j) => Json.obj(mId.category.name -> j) }
                .foldLeft(Json.obj("digest" -> Json.fromString(name.digest.value)))((a, b) => b.deepMerge(a))
            Json.obj(name.value -> inner)
          }
          .toList
        Json.obj(measurement -> Json.arr(arr*))
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

  // for std-out etc
  private def grouping2(f: Snapshot => (MetricID, Json)): Json =
    (snapshot.gauges.map(f) :::
      snapshot.counters.map(f) :::
      snapshot.timers.map(f) :::
      snapshot.meters.map(f) :::
      snapshot.histograms.map(f))
      .groupBy(_._1.metricName.measurement.value) // measurement group
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
          .map { case (n, j) => Json.obj(n.display -> j) }
        measurement -> Json.arr(arr*)
      }
      .toList
      .sortBy(_._1)
      .map { case (m, j) => Json.obj(m -> j) }
      .asJson

  def toPrettyJson(mp: MetricParams): Json = {
    val rateUnit = mp.rateUnitName
    val convert  = mp.rateConversion _
    grouping2 {
      case Snapshot.Counter(id, count) => id -> Json.fromLong(count)
      case Snapshot.Gauge(id, value)   => id -> value
      case Snapshot.Meter(id, data) =>
        val unit = data.unitShow
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(f"${convert(data.mean_rate.toHertz)}%2.2f $unit/$rateUnit"),
          "m1_rate" -> Json.fromString(f"${convert(data.m1_rate.toHertz)}%2.2f $unit/$rateUnit"),
          "m5_rate" -> Json.fromString(f"${convert(data.m5_rate.toHertz)}%2.2f $unit/$rateUnit"),
          "m15_rate" -> Json.fromString(f"${convert(data.m15_rate.toHertz)}%2.2f $unit/$rateUnit")
        )
      case Snapshot.Timer(id, data) =>
        id -> Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(f"${convert(data.mean_rate.toHertz)}%2.2f calls/$rateUnit"),
          "m1_rate" -> Json.fromString(f"${convert(data.m1_rate.toHertz)}%2.2f calls/$rateUnit"),
          "m5_rate" -> Json.fromString(f"${convert(data.m5_rate.toHertz)}%2.2f calls/$rateUnit"),
          "m15_rate" -> Json.fromString(f"${convert(data.m15_rate.toHertz)}%2.2f calls/$rateUnit"),
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
          "min" -> Json.fromString(f"${data.min}%d $unit"),
          "max" -> Json.fromString(f"${data.max}%d $unit"),
          "mean" -> Json.fromString(f"${data.mean}%2.2f $unit"),
          "stddev" -> Json.fromString(f"${data.stddev}%2.2f $unit"),
          "p50" -> Json.fromString(f"${data.p50}%2.2f $unit"),
          "p75" -> Json.fromString(f"${data.p75}%2.2f $unit"),
          "p95" -> Json.fromString(f"${data.p95}%2.2f $unit"),
          "p98" -> Json.fromString(f"${data.p98}%2.2f $unit"),
          "p99" -> Json.fromString(f"${data.p99}%2.2f $unit"),
          "p999" -> Json.fromString(f"${data.p999}%2.2f $unit")
        )
    }
  }
}
