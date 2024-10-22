package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Ior, Kleisli}
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind
import io.circe.Json
import io.circe.syntax.EncoderOps

import scala.util.Try

sealed trait NJRatio[F[_]] {

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

  def kleisli[A](f: A => Ior[Long, Long]): Kleisli[F, A, Unit]
}

private class NJRatioImpl[F[_]](private[this] val ref: Ref[F, Ior[Long, Long]]) extends NJRatio[F] {

  private[this] def update(ior: Ior[Long, Long]): F[Unit] = ref.update(_ |+| ior)

  override def incNumerator(numerator: Long): F[Unit]     = update(Ior.Left(numerator))
  override def incDenominator(denominator: Long): F[Unit] = update(Ior.Right(denominator))
  override def incBoth(numerator: Long, denominator: Long): F[Unit] =
    update(Ior.Both(numerator, denominator))

  override def kleisli[A](f: A => Ior[Long, Long]): Kleisli[F, A, Unit] =
    Kleisli(update).local(f)
}

object NJRatio {
  val translator: Ior[Long, Long] => Json = {
    case Ior.Left(_)  => Json.fromString("n/a")
    case Ior.Right(_) => Json.fromString("0.0%")
    case Ior.Both(a, b) =>
      if (b === 0) { Json.fromString("n/a") }
      else {
        val rounded: Float =
          BigDecimal(a * 100.0 / b).setScale(2, BigDecimal.RoundingMode.HALF_UP).toFloat
        Json.fromString(s"$rounded%")
      }
  }

  final class Builder private[guard] (
    measurement: Measurement,
    translator: Ior[Long, Long] => Json,
    isEnabled: Boolean,
    tag: MetricTag)
      extends EnableConfig[Builder] {

    def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), translator, isEnabled, tag)

    def withTranslator(translator: Ior[Long, Long] => Json) =
      new Builder(measurement, translator, isEnabled, tag)

    def enable(value: Boolean): Builder = new Builder(measurement, translator, value, tag)

    def withTag(tag: String): Builder = new Builder(measurement, translator, isEnabled, MetricTag(Some(tag)))

    private[guard] def build[F[_]: Async](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): Resource[F, NJRatio[F]] = {
      val metricName = MetricName(serviceParams, measurement, name)

      val F = Async[F]

      val impl: Resource[F, NJRatio[F]] = for {
        token <- Resource.eval(F.unique)
        metricID = MetricID(metricName, Category.Gauge(GaugeKind.Gauge, tag), token).identifier
        ref <- Resource.eval(F.ref(Ior.both(0L, 0L)))
        dispatcher <- Dispatcher.sequential[F]
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
      } yield new NJRatioImpl[F](ref)

      val dummy: Resource[F, NJRatio[F]] = Resource.pure(new NJRatio[F] {
        override def incNumerator(numerator: Long): F[Unit]               = F.unit
        override def incDenominator(denominator: Long): F[Unit]           = F.unit
        override def incBoth(numerator: Long, denominator: Long): F[Unit] = F.unit
        override def kleisli[A](f: A => Ior[Long, Long]): Kleisli[F, A, Unit] =
          Kleisli((_: A) => F.unit)
      })

      if (isEnabled) impl else dummy
    }
  }
}
