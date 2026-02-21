package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.effect.kernel.Clock
import cats.kernel.Eq
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.{Applicative, Hash}
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.Encoder
import io.circe.generic.JsonCodec
import monocle.macros.GenPrism

sealed trait CategoryKind extends EnumEntry with Product

object CategoryKind {
  sealed trait GaugeKind extends CategoryKind

  object GaugeKind extends Enum[GaugeKind] with CirceEnum[GaugeKind] {
    val values: IndexedSeq[GaugeKind] = findValues

    case object HealthCheck extends GaugeKind
    case object Ratio extends GaugeKind

    case object Gauge extends GaugeKind
  }

  sealed trait CounterKind extends CategoryKind

  object CounterKind extends Enum[CounterKind] with CirceEnum[CounterKind] {
    val values: IndexedSeq[CounterKind] = findValues

    case object Risk extends CounterKind

    case object Counter extends CounterKind
  }

  sealed trait MeterKind extends CategoryKind

  object MeterKind extends Enum[MeterKind] with CirceEnum[MeterKind] {
    val values: IndexedSeq[MeterKind] = findValues

    case object Meter extends MeterKind
  }

  sealed trait HistogramKind extends CategoryKind

  object HistogramKind extends Enum[HistogramKind] with CirceEnum[HistogramKind] {
    val values: IndexedSeq[HistogramKind] = findValues

    case object Histogram extends HistogramKind
  }

  sealed trait TimerKind extends CategoryKind

  object TimerKind extends Enum[TimerKind] with CirceEnum[TimerKind] {
    val values: IndexedSeq[TimerKind] = findValues

    case object Timer extends TimerKind
  }
}

@JsonCodec
final case class Squants(unitSymbol: String, dimensionName: String)

@JsonCodec
sealed trait Category extends Product {
  def kind: CategoryKind
}
object Category {
  import CategoryKind.*

  final case class Gauge(kind: GaugeKind) extends Category
  final case class Counter(kind: CounterKind) extends Category
  final case class Meter(kind: MeterKind, squants: Squants) extends Category
  final case class Histogram(kind: HistogramKind, squants: Squants) extends Category
  final case class Timer(kind: TimerKind) extends Category
}

/** Represents a uniquely identifiable metric within a single JVM runtime.
  *
  * Each `MetricName` instance is uniquely identified by the combination of:
  *   - `name`: the logical name of the metric.
  *   - `age`: a monotonic timestamp in nanoseconds from `Clock[F].monotonic`.
  *   - `uniqueToken`: an internal, hashed token generated via `Unique[F]`.
  *
  * Notes on `uniqueToken`:
  *   - It is private and cannot be set externally.
  *   - It acts as an internal safeguard to guarantee uniqueness even if two metrics are created at the exact
  *     same monotonic timestamp (extremely rare).
  *   - It participates in equality and hashing (via case class semantics), ensuring that no two `MetricName`
  *     instances are ever considered equal unless all three components match.
  */
@JsonCodec
final case class MetricName private (name: String, age: Long, private val uniqueToken: Int)
object MetricName {
  implicit val eqMetricName: Eq[MetricName] = Eq.fromUniversalEquals

  def apply[F[_]: Applicative](name: String)(implicit U: Unique[F], C: Clock[F]): F[MetricName] =
    (C.monotonic, U.unique).mapN((age, token) =>
      MetricName(name, age.toNanos, Hash[Unique.Token].hash(token)))
}

@JsonCodec
final case class MetricLabel(label: String, domain: Domain)

@JsonCodec
final case class MetricID(metricLabel: MetricLabel, metricName: MetricName, category: Category) {
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
  val isMeter: Option[Category.Meter] = GenPrism[Category, Category.Meter].getOption(category)
  val isHisto: Option[Category.Histogram] = GenPrism[Category, Category.Histogram].getOption(category)
}
