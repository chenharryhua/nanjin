package com.github.chenharryhua.nanjin.guard.action

import cats.Applicative
import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.toFunctorOps
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind

sealed trait NJCounter[F[_]] {
  def unsafeInc(num: Long): Unit
  def unsafeDec(num: Long): Unit

  def inc(num: Long): F[Unit]
  def dec(num: Long): F[Unit]

  final def kleisli[A](f: A => Long): Kleisli[F, A, Unit] =
    Kleisli(inc).local(f)
}

private class NJCounterImpl[F[_]: Sync](
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val isRisk: Boolean,
  private[this] val tag: MetricTag)
    extends NJCounter[F] {

  private[this] val F = Sync[F]

  private[this] lazy val (counter_name: String, counter: Counter) =
    if (isRisk) {
      val id = MetricID(name, Category.Counter(CounterKind.Risk, tag), token).identifier
      (id, metricRegistry.counter(id))
    } else {
      val id = MetricID(name, Category.Counter(CounterKind.Counter, tag), token).identifier
      (id, metricRegistry.counter(id))
    }

  override def unsafeInc(num: Long): Unit = counter.inc(num)
  override def unsafeDec(num: Long): Unit = counter.dec(num)

  override def inc(num: Long): F[Unit] = F.delay(unsafeInc(num))
  override def dec(num: Long): F[Unit] = F.delay(unsafeDec(num))

  val unregister: F[Unit] = F.delay {
    metricRegistry.remove(counter_name)
  }.void
}

object NJCounter {
  def dummy[F[_]](implicit F: Applicative[F]): NJCounter[F] =
    new NJCounter[F] {
      override def unsafeInc(num: Long): Unit = ()
      override def unsafeDec(num: Long): Unit = ()
      override def inc(num: Long): F[Unit]    = F.unit
      override def dec(num: Long): F[Unit]    = F.unit
    }

  final class Builder private[guard] (
    measurement: Measurement,
    isRisk: Boolean,
    isEnabled: Boolean,
    tag: MetricTag)
      extends MetricBuilder[Builder] {

    def asRisk: Builder = new Builder(measurement, true, isEnabled, tag)

    override def withMeasurement(measurement: String): Builder =
      new Builder(Measurement(measurement), isRisk, isEnabled, tag)

    override def enable(value: Boolean): Builder =
      new Builder(measurement, isRisk, value, tag)

    override def withTag(tag: String): Builder =
      new Builder(measurement, isRisk, isEnabled, MetricTag(tag))

    private[guard] def build[F[_]](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams)(implicit F: Sync[F]): Resource[F, NJCounter[F]] =
      if (isEnabled) {
        val metricName = MetricName(serviceParams, measurement, name)
        Resource
          .make(F.unique.map(new NJCounterImpl[F](_, metricName, metricRegistry, isRisk, tag)))(_.unregister)
      } else
        Resource.pure(dummy[F])
  }
}
