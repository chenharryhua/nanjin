package com.github.chenharryhua.nanjin.guard.metrics

import cats.Applicative
import cats.data.Ior
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.functor.toFunctorOps
import cats.syntax.group.catsSyntaxSemigroup
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.event.CategoryKind.GaugeKind
import com.github.chenharryhua.nanjin.guard.event.{Category, MetricID, MetricLabel, MetricName}
import io.circe.Json
import io.circe.syntax.EncoderOps

import scala.util.Try

trait Percentile[F[_]] extends KleisliLike[F, Ior[Long, Long]] {

  /** @param numerator
    *   The number above the fraction line, representing the part of the whole. For example, in the fraction
    *   3/4, 3 is the numerator.
    */
  def incNumerator(numerator: Long): F[Unit]

  /** @param denominator
    *   The number below the fraction line, representing the total number of equal parts. For example, in the
    *   fraction 3/4, 4 is the denominator.
    */
  def incDenominator(denominator: Long): F[Unit]

  /** @param numerator
    *   The number above the fraction line, representing the part of the whole. For example, in the fraction
    *   3/4, 3 is the numerator.
    * @param denominator
    *   The number below the fraction line, representing the total number of equal parts. For example, in the
    *   fraction 3/4, 4 is the denominator.
    */
  def incBoth(numerator: Long, denominator: Long): F[Unit]

  final override def run(ior: Ior[Long, Long]): F[Unit] = ior match {
    case Ior.Left(a)    => incNumerator(a)
    case Ior.Right(b)   => incDenominator(b)
    case Ior.Both(a, b) => incBoth(a, b)
  }
}

object Percentile {
  def noop[F[_]](implicit F: Applicative[F]): Percentile[F] =
    new Percentile[F] {
      override def incNumerator(numerator: Long): F[Unit] = F.unit
      override def incDenominator(denominator: Long): F[Unit] = F.unit
      override def incBoth(numerator: Long, denominator: Long): F[Unit] = F.unit
    }

  private class Impl[F[_]](private[this] val ref: Ref[F, Ior[Long, Long]]) extends Percentile[F] {

    private[this] def update(ior: Ior[Long, Long]): F[Unit] = ref.update(_ |+| ior)

    override def incNumerator(numerator: Long): F[Unit] = update(Ior.Left(numerator))
    override def incDenominator(denominator: Long): F[Unit] = update(Ior.Right(denominator))

    override def incBoth(numerator: Long, denominator: Long): F[Unit] =
      update(Ior.Both(numerator, denominator))

  }

  val translator: Ior[Long, Long] => Json = {
    case Ior.Left(_)    => Json.fromString("n/a")
    case Ior.Right(_)   => Json.fromString("0.0%")
    case Ior.Both(a, b) =>
      if (b === 0) { Json.fromString("n/a") }
      else {
        val rounded: Float =
          BigDecimal(a * 100.0 / b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
        Json.fromString(s"$rounded%")
      }
  }

  final class Builder private[guard] (
    isEnabled: Boolean,
    translator: Ior[Long, Long] => Json
  ) extends EnableConfig[Builder] {

    def withTranslator(translator: Ior[Long, Long] => Json): Builder =
      new Builder(isEnabled, translator)

    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, translator)

    private[guard] def build[F[_]: Async](
      label: MetricLabel,
      name: String,
      metricRegistry: MetricRegistry,
      dispatcher: Dispatcher[F]): Resource[F, Percentile[F]] = {

      val F = Async[F]

      val impl: Resource[F, Percentile[F]] = for {
        metricName <- Resource.eval(MetricName(name))
        metricID = MetricID(label, metricName, Category.Gauge(GaugeKind.Ratio)).identifier
        ref <- Resource.eval(F.ref(Ior.both(0L, 0L)))
        _ <- Resource.make(F.delay {
          metricRegistry.gauge(
            metricID,
            () =>
              new Gauge[Json] {
                override def getValue: Json =
                  Try(dispatcher.unsafeRunSync(ref.get.map(translator))).fold(_ => Json.Null, _.asJson)
              }
          )
        })(_ => F.delay(metricRegistry.remove(metricID)).void)
      } yield new Impl[F](ref)

      if (isEnabled) impl else Resource.pure(noop)
    }
  }
}
