package com.github.chenharryhua.nanjin.guard.translators

import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.config.MetricParams
import com.github.chenharryhua.nanjin.guard.event.{MetricSnapshot, Snapshot}
import io.circe.Json
import io.circe.syntax.EncoderOps

final private class SnapshotJson(snapshot: MetricSnapshot) {

  private def grouping(f: Snapshot => Json): Json =
    (snapshot.gauges.map(g => (g.id.metricName.measurement.value, g.id.show -> f(g))) :::
      snapshot.counters.map(c => (c.id.metricName.measurement.value, c.id.show -> f(c))) :::
      snapshot.timers.map(t => (t.id.metricName.measurement.value, t.id.show -> f(t))) :::
      snapshot.meters.map(m => (m.id.metricName.measurement.value, m.id.show -> f(m))) :::
      snapshot.histograms.map(h => (h.id.metricName.measurement.value, h.id.show -> f(h))))
      .groupBy(_._1)
      .map { case (name, lst) =>
        val js = Json.obj(lst.map(_._2)*)
        Json.obj(name -> js)
      }
      .asJson

  // for db
  def toVanillaJson: Json =
    grouping {
      case Snapshot.Counter(_, count)  => Json.fromLong(count)
      case Snapshot.Gauge(_, value)    => value
      case Snapshot.Meter(_, data)     => data.asJson
      case Snapshot.Timer(_, data)     => data.asJson
      case Snapshot.Histogram(_, data) => data.asJson
    }

  // for std-out, slack etc
  def toPrettyJson(mp: MetricParams): Json = {
    val rateUnit = mp.rateUnitName
    val convert  = mp.rateConversion _
    grouping {
      case Snapshot.Counter(_, count) => Json.fromLong(count)
      case Snapshot.Gauge(_, value)   => value
      case Snapshot.Meter(_, data) =>
        val unit = data.unitShow
        Json.obj(
          "count" -> Json.fromLong(data.count),
          "mean_rate" -> Json.fromString(f"${convert(data.mean_rate.toHertz)}%2.2f $unit/$rateUnit"),
          "m1_rate" -> Json.fromString(f"${convert(data.m1_rate.toHertz)}%2.2f $unit/$rateUnit"),
          "m5_rate" -> Json.fromString(f"${convert(data.m5_rate.toHertz)}%2.2f $unit/$rateUnit"),
          "m15_rate" -> Json.fromString(f"${convert(data.m15_rate.toHertz)}%2.2f $unit/$rateUnit")
        )
      case Snapshot.Timer(_, data) =>
        Json.obj(
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

      case Snapshot.Histogram(_, data) =>
        val unit = data.unitShow
        Json.obj(
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
