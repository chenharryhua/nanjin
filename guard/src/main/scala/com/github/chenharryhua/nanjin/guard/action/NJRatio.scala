package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Ior, Kleisli}
import cats.effect.kernel.{Async, Ref, Resource, Unique}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.GaugeKind
import io.circe.Json
import io.circe.syntax.EncoderOps

import scala.util.Try

final class NJRatio[F[_]] private (private[this] val ref: Ref[F, Ior[Double, Double]]) {

  private[this] def update[A, B](ior: Ior[A, B])(implicit NA: Numeric[A], NB: Numeric[B]): F[Unit] = {
    val dab: Ior[Double, Double] = ior match {
      case Ior.Left(a)    => Ior.Left(NA.toDouble(a))
      case Ior.Right(b)   => Ior.Right(NB.toDouble(b))
      case Ior.Both(a, b) => Ior.Both(NA.toDouble(a), NB.toDouble(b))
    }
    ref.update(_ |+| dab)
  }

  /** @param numerator
    *   The number above the fraction line, representing the part of the whole. For example, in the fraction
    *   3/4, 3 is the numerator.
    */
  def incNumerator[A: Numeric](numerator: A): F[Unit] = update(Ior.Left(numerator))

  /** @param denominator
    *   The number below the fraction line, representing the total number of equal parts. For example, in the
    *   fraction 3/4, 4 is the denominator.
    */
  def incDenominator[A: Numeric](denominator: A): F[Unit] = update(Ior.Right(denominator))

  /** @param numerator
    *   The number above the fraction line, representing the part of the whole. For example, in the fraction
    *   3/4, 3 is the numerator.
    * @param denominator
    *   The number below the fraction line, representing the total number of equal parts. For example, in the
    *   fraction 3/4, 4 is the denominator.
    */
  def incBoth[A: Numeric, B: Numeric](numerator: A, denominator: B): F[Unit] =
    update(Ior.Both(numerator, denominator))

  def kleisli[A](f: A => Ior[Double, Double]): Kleisli[F, A, Unit] =
    Kleisli(update[Double, Double]).local(f)
}

object NJRatio {

  final class Builder private[guard] (measurement: Measurement) {

    def withMeasurement(measurement: String): Builder = new Builder(Measurement(measurement))

    private[guard] def build[F[_]: Async](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams,
      token: Option[Unique.Token]): Resource[F, NJRatio[F]] = {
      val metricName = MetricName(serviceParams, measurement, name)

      val F = Async[F]

      for {
        unique <- token.fold(Resource.eval(F.unique))(Resource.pure)
        metricID = MetricID(metricName, Category.Gauge(GaugeKind.Ratio), unique.hash).identifier
        ref <- Resource.eval(F.ref(Ior.Left(0.0d): Ior[Double, Double]))
        dispatcher <- Dispatcher.sequential[F]
        _ <- Resource.make(F.delay {
          metricRegistry.gauge(
            metricID,
            () =>
              new Gauge[Json] {
                private val calc: F[Option[Double]] = ref.get.map {
                  case Ior.Left(_)  => None
                  case Ior.Right(_) => Some(0.0)
                  case Ior.Both(a, b) =>
                    if (b === 0.0) {
                      None
                    } else {
                      val rounded =
                        BigDecimal((a / b) * 100.0).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
                      Some(rounded)
                    }
                }

                override def getValue: Json =
                  Try(dispatcher.unsafeRunSync(calc)).fold(_ => Json.Null, _.asJson)
              }
          )
        })(_ => F.delay(metricRegistry.remove(metricID)).void)
      } yield new NJRatio[F](ref)
    }
  }
}
