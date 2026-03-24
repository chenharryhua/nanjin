package com.github.chenharryhua.nanjin.guard.event

import cats.effect.Unique
import cats.effect.kernel.Clock
import cats.kernel.Eq
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.{Applicative, Hash}
import io.circe.{Codec, Decoder, Encoder}
import monocle.macros.GenPrism
import squants.{Quantity, UnitOfMeasure}

sealed trait CategoryKind extends Product

enum GaugeKind extends CategoryKind derives Encoder, Decoder:
  case Gauge, HealthCheck, Ratio

enum CounterKind extends CategoryKind derives Encoder, Decoder:
  case Counter, Risk

enum MeterKind extends CategoryKind derives Encoder, Decoder:
  case Meter

enum HistogramKind extends CategoryKind derives Encoder, Decoder:
  case Histogram

enum TimerKind extends CategoryKind derives Encoder, Decoder:
  case Timer

enum Category derives Encoder, Decoder:
  case Gauge(kind: GaugeKind) extends Category
  case Counter(kind: CounterKind) extends Category
  case Meter(kind: MeterKind, squants: Squants) extends Category
  case Histogram(kind: HistogramKind, squants: Squants) extends Category
  case Timer(kind: TimerKind) extends Category

final case class Squants private (unitSymbol: String, dimensionName: String) derives Codec.AsObject
object Squants {
  def apply[A <: Quantity[A]](um: UnitOfMeasure[A]): Squants =
    Squants(um.symbol, um(1).dimension.name)
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
final case class MetricName private (name: String, age: Long, private val uniqueToken: Int)
    derives Codec.AsObject
object MetricName {
  given Eq[MetricName] = Eq.fromUniversalEquals

  def apply[F[_]: Applicative](name: String)(using U: Unique[F], C: Clock[F]): F[MetricName] =
    (C.monotonic, U.unique).mapN((age, token) =>
      MetricName(name, age.toNanos, Hash[Unique.Token].hash(token)))
}

final case class MetricLabel(label: String, domain: Domain) derives Codec.AsObject

final case class MetricID(metricLabel: MetricLabel, metricName: MetricName, category: Category)
    derives Codec.AsObject {
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
  val isMeter: Option[Category.Meter] = GenPrism[Category, Category.Meter].getOption(category)
  val isHisto: Option[Category.Histogram] = GenPrism[Category, Category.Histogram].getOption(category)
}
