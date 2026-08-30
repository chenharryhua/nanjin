package com.github.chenharryhua.nanjin.guard.metrics

import cats.derived.derived
import cats.effect.Unique
import cats.effect.kernel.Clock
import cats.kernel.Eq
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.{Applicative, Hash}
import com.github.chenharryhua.nanjin.guard.config.{Domain, Service, Task}
import io.circe.{Codec, Decoder, Encoder}
import org.typelevel.otel4s.Attribute
import squants.{Each, Quantity, UnitOfMeasure}

sealed trait MetricKind extends Product
object MetricKind:
  enum Gauge extends MetricKind derives Encoder, Decoder:
    case Default, HealthCheck, Percentile

  enum Counter extends MetricKind derives Encoder, Decoder:
    case Default, Risk

  enum Meter extends MetricKind derives Encoder, Decoder:
    case Default

  enum Histogram extends MetricKind derives Encoder, Decoder:
    case Default

  enum Timer extends MetricKind derives Encoder, Decoder:
    case Default
end MetricKind

enum MetricCategory derives Encoder, Decoder:
  case Gauge(kind: MetricKind.Gauge, isCached: Boolean)
  case Counter(kind: MetricKind.Counter)
  case Meter(kind: MetricKind.Meter, squants: Squants)
  case Histogram(kind: MetricKind.Histogram, squants: Squants)
  case Timer(kind: MetricKind.Timer, squants: Squants = Squants(Each))
end MetricCategory

final case class Squants private (unitSymbol: String, dimensionName: String) derives Codec.AsObject
private object Squants:
  def apply[A <: Quantity[A]](um: UnitOfMeasure[A]): Squants =
    Squants(um.symbol, um(1).dimension.name)
end Squants

final case class MetricName private (name: String, age: Long, uniqueToken: Int) derives Eq, Codec.AsObject
private object MetricName:
  def apply[F[_]: {Applicative, Clock, Unique}](name: String): F[MetricName] =
    (Clock[F].monotonic, Unique[F].unique).mapN((age, token) =>
      MetricName(name, age.toNanos, Hash[Unique.Token].hash(token)))
end MetricName

final case class MetricLabel(label: String, domain: Domain, service: Service, task: Task)
    derives Codec.AsObject

final case class MetricID(metricLabel: MetricLabel, metricName: MetricName, category: MetricCategory)
    derives Codec.AsObject:
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
  val attributes: List[Attribute[String]] = {
    val cat = category match {
      case MetricCategory.Gauge(kind, _)     => kind.productPrefix
      case MetricCategory.Counter(kind)      => kind.productPrefix
      case MetricCategory.Meter(kind, _)     => kind.productPrefix
      case MetricCategory.Histogram(kind, _) => kind.productPrefix
      case MetricCategory.Timer(kind, _)     => kind.productPrefix
    }
    // Only per-metric dimensions belong here. Emitter identity (task/service/serviceId) is Resource
    // information in OpenTelemetry and must be set on the SDK Resource by the caller (see
    // ServiceConfig.withMeterProvider), not stamped onto every measurement.
    List(
      Attribute.from("domain", metricLabel.domain.value),
      Attribute.from("category", cat.toLowerCase(java.util.Locale.ROOT))
    )
  }
end MetricID
