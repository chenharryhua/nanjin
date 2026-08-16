package com.github.chenharryhua.nanjin.guard.metrics

import cats.derived.derived
import cats.effect.Unique
import cats.effect.kernel.Clock
import cats.kernel.Eq
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.{Applicative, Hash}
import com.github.chenharryhua.nanjin.guard.config.Domain
import io.circe.{Codec, Decoder, Encoder}
import squants.{Quantity, UnitOfMeasure}

final case class Squants private (unitSymbol: String, dimensionName: String) derives Codec.AsObject
object Squants:
  def apply[A <: Quantity[A]](um: UnitOfMeasure[A]): Squants =
    Squants(um.symbol, um(1).dimension.name)
end Squants

final case class MetricName private (name: String, age: Long, uniqueToken: Int) derives Eq, Codec.AsObject
private object MetricName:
  def apply[F[_]: Applicative](name: String)(using U: Unique[F], C: Clock[F]): F[MetricName] =
    (C.monotonic, U.unique).mapN((age, token) =>
      MetricName(name, age.toNanos, Hash[Unique.Token].hash(token)))
end MetricName

final case class MetricLabel(label: String, domain: Domain) derives Codec.AsObject

final case class MetricID(metricLabel: MetricLabel, metricName: MetricName, category: Category)
    derives Codec.AsObject:
  val identifier: String = Encoder[MetricID].apply(this).noSpaces
  val squants: Option[Squants] = category match {
    case Category.Gauge(_)              => None
    case Category.Counter(_)            => None
    case Category.Meter(_, squants)     => Some(squants)
    case Category.Histogram(_, squants) => Some(squants)
    case Category.Timer(_)              => None
  }
end MetricID
