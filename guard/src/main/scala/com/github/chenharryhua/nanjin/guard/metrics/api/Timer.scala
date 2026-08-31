package com.github.chenharryhua.nanjin.guard.metrics.api

import cats.data.ContT
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
  MetricId,
  MetricKind,
  MetricScope,
  MetricToken
}
import org.typelevel.otel4s.metrics.{BucketBoundaries, Histogram as OtelHistogram, MeterProvider}
import squants.time.Nanoseconds

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
    scope: MetricScope,
    metricRegistry: MetricRegistry,
    reservoir: Option[Reservoir],
    name: MetricToken,
    otel: OtelHistogram[F, Double],
    timeunit: squants.time.TimeUnit
  )(implicit F: Sync[F])
      extends Timer[F] {

    private val id: MetricId =
      MetricId(scope, name, MetricCategory.Timer(MetricKind.Timer.Default))

    private val supplier: MetricRegistry.MetricSupplier[CodahaleTimer] = () =>
      reservoir match {
        case Some(value) => new CodahaleTimer(value)
        case None        => new CodahaleTimer(new ExponentiallyDecayingReservoir) // default reservoir
      }

    private val timer: CodahaleTimer = metricRegistry.timer(id.identifier, supplier)

    // Records to Dropwizard in nanoseconds and to an otel4s duration Histogram in the configured time unit
    // (no-op when the configured MeterProvider is MeterProvider.noop). The elapsed nanoseconds are converted
    // to `timeunit`, and the otel instrument carries that unit's symbol.
    override def elapsedNano(num: Long): F[Unit] =
      F.delay(timer.update(num, TimeUnit.NANOSECONDS)) >>
        otel.record(Nanoseconds(num).in(timeunit).value, id.attributes)

    // Measure the effect once, then record the same elapsed time to both backends.
    override def timing[A](fa: F[A]): F[A] =
      F.timed(fa).flatMap { case (fd, result) => elapsedNano(fd.toNanos).as(result) }

    val unregister: F[Unit] = F.delay(metricRegistry.remove(id.identifier)).void

  }

  final class Builder private[Timer] (
    isEnabled: Boolean,
    reservoir: Option[Reservoir],
    description: Option[String],
    boundaries: Option[BucketBoundaries],
    timeunit: squants.time.TimeUnit
  ) extends EnableConfig[Builder] {

    /** Choose the Dropwizard reservoir used to retain timing observations. */
    def withReservoir(reservoir: Reservoir): Builder =
      new Builder(isEnabled, Some(reservoir), description, boundaries, timeunit)

    /** Attach a human-readable description carried by the OpenTelemetry instrument. */
    def withDescription(description: String): Builder =
      new Builder(isEnabled, reservoir, Some(description), boundaries, timeunit)

    /** Choose the time unit the OpenTelemetry duration histogram records in (default seconds). The elapsed
      * nanoseconds are converted to this unit and the instrument carries its symbol.
      */
    def withTimeUnit(timeunit: squants.time.TimeUnit): Builder =
      new Builder(isEnabled, reservoir, description, boundaries, timeunit)

    /** Enable or disable metric registration; disabled timers become no-ops. */
    override def enable(isEnabled: Boolean): Builder =
      new Builder(isEnabled, reservoir, description, boundaries, timeunit)

    private[Timer] def build[F[_]](
      scope: MetricScope,
      name: String,
      metricRegistry: MetricRegistry,
      meterProvider: MeterProvider[F])(using F: Sync[F]): Resource[F, Timer[F]] = {
      def timer: Resource[F, Timer[F]] =
        for {
          otel <- Resource.eval(meterProvider.get(scope.label).flatMap { m =>
            ContT.pure(m.histogram[Double](name).withUnit(timeunit.symbol))
              .map(b => boundaries.fold(b)(b.withExplicitBucketBoundaries))
              .map(b => description.fold(b)(b.withDescription))
              .run(_.create)
          })
          t <- Resource.make(
            MetricToken(name).map(Impl[F](scope, metricRegistry, reservoir, _, otel, timeunit)))(_.unregister)
        } yield t

      if isEnabled then timer else noop.pure
    }
  }

  private[metrics] def apply[F[_]: Sync](
    mr: MetricRegistry,
    scope: MetricScope,
    name: String,
    meterProvider: MeterProvider[F],
    f: Endo[Builder]): Resource[F, Timer[F]] =
    f(
      new Builder(
        isEnabled = true,
        reservoir = None,
        description = None,
        boundaries = None,
        timeunit = squants.Seconds))
      .build[F](scope, name, mr, meterProvider)
}
