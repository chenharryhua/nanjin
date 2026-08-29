package com.github.chenharryhua.nanjin.guard.metrics.api

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.{Applicative, Endo}
import com.codahale.metrics.{
  ExponentiallyDecayingReservoir,
  MetricRegistry,
  Reservoir,
  Timer as CodahaleTimer
}
import com.github.chenharryhua.nanjin.common.EnableConfig
import com.github.chenharryhua.nanjin.guard.metrics.{
  MetricCategory,
  MetricID,
  MetricKind,
  MetricLabel,
  MetricName
}
import org.typelevel.otel4s.metrics.{Histogram as OtelHistogram, MeterProvider}

import java.time.Duration as JavaDuration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

/** Converts a duration-like value to nanoseconds for timer updates. */
trait ToNanos[A]:
  /** Convert a duration-like value to nanoseconds. */
  def apply(a: A): Long
object ToNanos:
  given ToNanos[FiniteDuration] = _.toNanos
  given ToNanos[JavaDuration] = _.toNanos
end ToNanos

/** Effectful timer for recording durations or timing an effect. */
trait Timer[F[_]]:
  /** Record an elapsed duration already expressed in nanoseconds. */
  def elapsedNano(num: Long): F[Unit]

  /** Run `fa` and record its elapsed time, preserving its result and error. */
  def timing[A](fa: F[A]): F[A]

  final def elapsedNano(num: Int): F[Unit] =
    elapsedNano(num.toLong)
  final def elapsed[A: ToNanos](nano: A): F[Unit] =
    elapsedNano(summon[ToNanos[A]](nano))
end Timer

object Timer {

  def noop[F[_]: Applicative]: Timer[F] = new Timer[F] {
    override def elapsedNano(num: Long): F[Unit] = ().pure
    override def timing[A](fa: F[A]): F[A] = fa
  }

  private class Impl[F[_]](
    label: MetricLabel,
    metricRegistry: MetricRegistry,
    reservoir: Option[Reservoir],
    name: MetricName,
    otel: OtelHistogram[F, Double]
  )(implicit F: Sync[F])
      extends Timer[F] {

    private val id: MetricID =
      MetricID(label, name, MetricCategory.Timer(MetricKind.Timer.Default))

    private val supplier: MetricRegistry.MetricSupplier[CodahaleTimer] = () =>
      reservoir match {
        case Some(value) => new CodahaleTimer(value)
        case None        => new CodahaleTimer(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private val timer: CodahaleTimer = metricRegistry.timer(id.identifier, supplier)

    // Records to Dropwizard (nanoseconds) and to an otel4s duration Histogram in seconds (no-op when the
    // configured MeterProvider is MeterProvider.noop).
    override def elapsedNano(num: Long): F[Unit] =
      F.delay(timer.update(num, TimeUnit.NANOSECONDS)) >> otel.record(num.toDouble / 1e9, id.attributes)

    // Measure the effect once, then record the same elapsed time to both backends.
    override def timing[A](fa: F[A]): F[A] =
      F.timed(fa).flatMap { case (fd, result) => elapsedNano(fd.toNanos).as(result) }

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  final class Builder private[Timer] (
    isEnabled: Boolean,
    reservoir: Option[Reservoir],
    description: String
  ) extends EnableConfig[Builder] {

    /** Choose the Dropwizard reservoir used to retain timing observations. */
    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, Some(reservoir), description)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, reservoir, description)

    /** Enable or disable metric registration; disabled timers become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, reservoir, description)

    private[Timer] def build[F[_]](
      label: MetricLabel,
      name: String,
      metricRegistry: MetricRegistry,
      meterProvider: MeterProvider[F])(using F: Sync[F]): Resource[F, Timer[F]] = {
      def timer: Resource[F, Timer[F]] =
        for {
          // Timer maps to a duration histogram in seconds (OpenTelemetry duration convention).
          otel <- Resource.eval(
            meterProvider.get(label.label).flatMap(
              _.histogram[Double](name).withUnit("s").withDescription(description).create))
          t <- Resource.make(MetricName(name).map(Impl[F](label, metricRegistry, reservoir, _, otel)))(
            _.unregister)
        } yield t

      if isEnabled then timer else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    label: MetricLabel,
    name: String,
    meterProvider: MeterProvider[F],
    f: Endo[Builder]): Resource[F, Timer[F]] =
    f(new Builder(isEnabled = true, reservoir = None, description = ""))
      .build[F](label, name, mr, meterProvider)
}
