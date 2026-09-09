package com.github.chenharryhua.nanjin.guard.translator

import cats.data.NonEmptyList
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.functorFilter.toFunctorFilterOps
import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.guard.config.NbspChar
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.Snapshot
import com.github.chenharryhua.nanjin.guard.metrics.{MetricId, Squants}
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.apache.commons.lang3.StringUtils
import squants.time

import java.text.DecimalFormat

/** Selects the character used to indent rendered output.
  *
  *   - `Nbsp`: a non-breaking space (`NbspChar`), for targets that collapse ordinary whitespace (e.g. Teams
  *     Adaptive Cards, HTML).
  *   - `Normal`: an ordinary space, for plain-text targets.
  */
enum IndentSpace:
  case Nbsp, Normal

/** Maps an `IndentSpace` to its concrete indent character. */
private def indentSpace(is: IndentSpace): Char = is match {
  case IndentSpace.Nbsp   => NbspChar
  case IndentSpace.Normal => ' '
}

/** Renders a metrics `Snapshot` into several human- or machine-facing presentation formats.
  *
  * The same snapshot can be emitted as:
  *   - `toVanillaJson`: JSON keyed by encoder-derived field names, suitable for persistence.
  *   - `toPrettyJson`: JSON with unit-formatted string values for screen display; null gauges are dropped.
  *   - `toYaml`: a homemade YAML-ish string for screen display.
  *
  * All three group metrics by domain, then by scope, ordering entries by metric age so output is stable and
  * reads oldest-first. The private `*_str`/`meters`/`timers`/`histograms` helpers turn each metric family
  * into label/value rows with unit-aware formatting; the JSON and YAML paths then assemble those rows.
  *
  * @param snapshot
  *   the metrics snapshot to render
  * @param indent
  *   the indentation style (space vs non-breaking space); defaults to `IndentSpace.Normal`
  */
final private[guard] class SnapshotPolyglot(snapshot: Snapshot, indent: IndentSpace = IndentSpace.Normal) {
  private val space: Char = indentSpace(indent)
  private val space2: String = String.valueOf(space) * 2
  private val space4: String = space2 * 2
  private val decimalFormatter: DecimalFormat = new DecimalFormat(decimalFormat)

  /** Renders a per-second rate using the largest time unit that keeps the value above 1, so a slow rate reads
    * as e.g. `"5 x/day"` rather than a tiny per-second figure. Falls through seconds -> minutes -> hours ->
    * days.
    */
  private def adaptable_mean_rate(data: Double, symbol: String): String =
    if (data > 1)
      s"${decimalFormatter.format(data)} $symbol/${time.Seconds.symbol}"
    else if (data * 60 > 1)
      s"${decimalFormatter.format(data * 60)} $symbol/${time.Minutes.symbol}"
    else if (data * 3600 > 1)
      s"${decimalFormatter.format(data * 3600)} $symbol/${time.Hours.symbol}"
    else
      s"${decimalFormatter.format(data * 86400)} $symbol/${time.Days.symbol}"

  /** Formatted label/value rows for each meter: aggregate plus mean and 1/5/15-minute rates, using the
    * meter's own unit symbol.
    */
  private def meters: List[(MetricId, NonEmptyList[(String, String)])] =
    snapshot.meters.map { m =>
      val unit = m.meter.squants.unitSymbol
      m.metricId -> NonEmptyList.of(
        "aggregate" -> s"${decimalFormatter.format(m.meter.aggregate)} $unit",
        "mean_rate" -> adaptable_mean_rate(m.meter.mean_rate.toHertz, unit),
        "m1_rate" -> s"${decimalFormatter.format(m.meter.m1_rate.toHertz)} $unit/s",
        "m5_rate" -> s"${decimalFormatter.format(m.meter.m5_rate.toHertz)} $unit/s",
        "m15_rate" -> s"${decimalFormatter.format(m.meter.m15_rate.toHertz)} $unit/s"
      )
    }

  /** Formatted label/value rows for each timer: invocation count, rates, and the min/max/mean plus percentile
    * durations, with durations rendered via the shared duration formatter.
    */
  private def timers: List[(MetricId, NonEmptyList[(String, String)])] =
    snapshot.timers.map { t =>
      val unit = s"calls/${time.Seconds.symbol}"
      t.metricId -> NonEmptyList.of(
        "invocations" -> decimalFormatter.format(t.timer.calls),
        "mean_rate" -> adaptable_mean_rate(t.timer.mean_rate.toHertz, "calls"),
        "m1_rate" -> s"${decimalFormatter.format(t.timer.m1_rate.toHertz)} $unit",
        "m5_rate" -> s"${decimalFormatter.format(t.timer.m5_rate.toHertz)} $unit",
        "m15_rate" -> s"${decimalFormatter.format(t.timer.m15_rate.toHertz)} $unit",
        "min" -> fmt.format(t.timer.min),
        "max" -> fmt.format(t.timer.max),
        "mean" -> fmt.format(t.timer.mean),
        "stddev" -> fmt.format(t.timer.stddev),
        "p50" -> fmt.format(t.timer.p50),
        "p75" -> fmt.format(t.timer.p75),
        "p95" -> fmt.format(t.timer.p95),
        "p98" -> fmt.format(t.timer.p98),
        "p99" -> fmt.format(t.timer.p99),
        "p999" -> fmt.format(t.timer.p999)
      )
    }

  /** Formats a single histogram statistic according to its `Squants` dimension: time-dimensioned values are
    * converted from their unit symbol and rendered via the duration formatter; all other dimensions render as
    * the formatted number followed by the unit symbol. An unrecognized time symbol produces a diagnostic
    * string rather than throwing.
    */
  private def interpret_histogram[A: Numeric](squants: Squants, data: A): String = {
    val unitSymbol: String = squants.unitSymbol
    val dimensionName: String = squants.dimensionName
    if (dimensionName === time.Time.name) {
      unitSymbol match {
        case time.Nanoseconds.symbol  => fmt.format(time.Nanoseconds(data))
        case time.Microseconds.symbol => fmt.format(time.Microseconds(data))
        case time.Milliseconds.symbol => fmt.format(time.Milliseconds(data))
        case time.Seconds.symbol      => fmt.format(time.Seconds(data))
        case time.Minutes.symbol      => fmt.format(time.Minutes(data))
        case time.Hours.symbol        => fmt.format(time.Hours(data))
        case time.Days.symbol         => fmt.format(time.Days(data))
        case unknown                  => s"$unknown - unknown symbol of dimension $dimensionName"
      }
    } else
      s"${decimalFormatter.format(data)} $unitSymbol"
  }

  /** Formatted label/value rows for each histogram: update count plus min/max/mean/stddev and percentiles,
    * each rendered by `interpret_histogram`.
    */
  private def histograms: List[(MetricId, NonEmptyList[(String, String)])] =
    snapshot.histograms.map { h =>
      val histo = h.histogram
      h.metricId -> NonEmptyList.of(
        "updates" -> decimalFormatter.format(histo.updates),
        "min" -> interpret_histogram(histo.squants, histo.min),
        "max" -> interpret_histogram(histo.squants, histo.max),
        "mean" -> interpret_histogram(histo.squants, histo.mean),
        "stddev" -> interpret_histogram(histo.squants, histo.stddev),
        "p50" -> interpret_histogram(histo.squants, histo.p50),
        "p75" -> interpret_histogram(histo.squants, histo.p75),
        "p95" -> interpret_histogram(histo.squants, histo.p95),
        "p98" -> interpret_histogram(histo.squants, histo.p98),
        "p99" -> interpret_histogram(histo.squants, histo.p99),
        "p999" -> interpret_histogram(histo.squants, histo.p999)
      )
    }

  /** Collapses each metric's label/value rows into a single JSON object, merging the per-row objects so all
    * labels sit under one object per metric.
    */
  private def json_list(lst: List[(MetricId, NonEmptyList[(String, String)])]): List[(MetricId, Json)] =
    lst.map { case (id, items) =>
      id -> items.map { case (key, js) => Json.obj(key -> Json.fromString(js)) }.toList.reduce[Json]((a, b) =>
        b.deepMerge(a))
    }

  /** Assembles per-metric JSON into the final tree: grouped by domain, then by scope within each domain, with
    * metrics ordered by age (oldest first). Each scope becomes an object keyed by its label, and each domain
    * becomes an array of those scope objects. The overall ordering is driven by the minimum age within each
    * group.
    */
  private def group_json(pairs: List[(MetricId, Json)]): Json =
    pairs
      .groupBy(_._1.scope.domain) // domain group
      .toList
      .map { case (domain, lst) =>
        val arr: List[Json] = lst
          .groupBy(_._1.scope) // group by metric scope
          .toList
          .map { case (scope, items) =>
            val age = items.map(_._1.token.age).min
            val inner: Json =
              items
                .sortBy(_._1.token.age)
                .map { case (mId, js) => Json.obj(mId.token.metricName -> js) }
                .reduce((a, b) => b.deepMerge(a))

            age -> Json.obj(scope.label.value -> inner.asJson)
          }
          .sortBy(_._1)
          .map(_._2)
        val age = lst.map(_._1.token.age).min
        (age, domain.value) -> Json.obj(domain.value -> Json.arr(arr*))
      }
      .sortBy(_._1)
      .map(_._2)
      .asJson

  /** JSON view intended for persistence (e.g. a database): every metric family is encoded with its own circe
    * `Encoder`, keeping raw numeric values, then grouped by domain and scope.
    */
  // for database etc
  def toVanillaJson: Json = {
    val counters = snapshot.counters.map(c => c.metricId -> c.counter.asJson)
    val gauges = snapshot.gauges.map(g => g.metricId -> g.gauge.value)
    val meters = snapshot.meters.map(m => m.metricId -> m.meter.asJson)
    val histograms = snapshot.histograms.map(h => h.metricId -> h.histogram.asJson)
    val timers = snapshot.timers.map(t => t.metricId -> t.timer.asJson)
    group_json(counters ::: gauges ::: meters ::: histograms ::: timers)
  }

  /** JSON view intended for screen display: counters and gauges keep their raw JSON, while meters,
    * histograms, and timers are rendered as unit-formatted strings. Null-valued gauges are omitted. Grouped
    * by domain and scope.
    */
  // for screen display
  def toPrettyJson: Json = {
    val counters: List[(MetricId, Json)] =
      snapshot.counters.map(c => c.metricId -> c.counter.asJson)
    val gauges: List[(MetricId, Json)] =
      snapshot.gauges.mapFilter(g =>
        if (g.gauge.value === Json.Null) None else Some(g.metricId -> g.gauge.value))

    val lst: List[(MetricId, Json)] =
      counters ::: gauges ::: json_list(meters ::: histograms ::: timers)
    group_json(lst)
  }

  /** Homemade Yaml
    */

  /** One YAML line per counter: `metricName: formattedCount`. */
  private def counter_str: List[(MetricId, List[String])] =
    snapshot.counters
      .map(c =>
        c.metricId -> List(show"${c.metricId.token.metricName}: ${decimalFormatter.format(c.counter)}"))

  /** YAML lines for each gauge, rendered via `JsonView.yml`. Gauges that render to nothing (e.g. null) are
    * dropped.
    */
  private def gauge_str: List[(MetricId, List[String])] =
    snapshot.gauges.mapFilter { g =>
      val content = JsonView.yml(g.metricId.token.metricName, g.gauge.value, space)
      if (content.isEmpty) None
      else
        Some(g.metricId -> content)
    }

  /** Renders label/value rows as `key: value` lines indented four spaces, with keys left-padded to the widest
    * key so values align.
    */
  private def padded(data: NonEmptyList[(String, String)]): NonEmptyList[String] = {
    val pad = data.map(_._1.length).toList.max
    data.map { case (k, v) => s"$space4${StringUtils.leftPad(k, pad, space)}: $v" }
  }

  /** Prefixes a metric's rendered lines with a `metricName:` header line. */
  private def named(id: MetricId, data: NonEmptyList[String]): List[String] =
    s"${id.token.metricName}:" :: data.toList

  /** YAML lines for each meter: a name header over aligned rate rows. */
  private def meter_str: List[(MetricId, List[String])] =
    meters.map { case (id, data) => id -> named(id, padded(data)) }

  /** YAML lines for each timer: a name header over aligned rate/duration rows. */
  private def timer_str: List[(MetricId, List[String])] =
    timers.map { case (id, data) => id -> named(id, padded(data)) }

  /** YAML lines for each histogram: a name header over aligned statistic rows. */
  private def histogram_str: List[(MetricId, List[String])] =
    histograms.map { case (id, data) => id -> named(id, padded(data)) }

  /** Assembles per-metric YAML lines into the final document: grouped by domain, then by scope (each
    * introduced by a `- label:` bullet), with metrics ordered by age (oldest first) and indentation applied
    * per level. Ordering within and across groups is driven by minimum age.
    */
  private def group_yaml(pairs: List[(MetricId, List[String])]): List[String] =
    pairs
      .groupBy(_._1.scope.domain) // domain group
      .toList
      .map { case (domain, domains) =>
        val arr: List[String] = domains
          .groupBy(_._1.scope) // group by metric scope
          .toList
          .map { case (scope, items) =>
            val age = items.map(_._1.token.age).min
            (age, scope) -> items.sortBy(_._1.token.age).flatMap(_._2.map(space4 + _))
          }
          .sortBy(_._1._1)
          .flatMap { case ((_, scope), items) =>
            s"$space2- ${scope.label}:" :: items
          }
        val age = domains.map(_._1.token.age).min
        ((age, domain.value), show"[$domain]:" :: arr)
      }
      .sortBy(_._1)
      .flatMap(_._2)

  /** Homemade YAML view intended for screen display: counters, gauges, meters, histograms, and timers
    * rendered as aligned lines, grouped by domain and scope, joined with newlines.
    */
  // for screen display
  def toYaml: String = {
    val lst: List[(MetricId, List[String])] =
      counter_str ::: gauge_str ::: meter_str ::: histogram_str ::: timer_str
    group_yaml(lst).mkString("\n")
  }
}
