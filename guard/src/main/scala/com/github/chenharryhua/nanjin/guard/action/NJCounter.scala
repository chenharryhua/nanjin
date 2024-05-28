package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Resource, Sync, Unique}
import cats.implicits.{catsSyntaxHash, toFunctorOps}
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.CounterKind

final class NJCounter[F[_]: Sync] private (
  private[this] val token: Unique.Token,
  private[this] val name: MetricName,
  private[this] val metricRegistry: MetricRegistry,
  private[this] val isRisk: Boolean) {

  private[this] val F = Sync[F]

  private[this] lazy val (counter_name: String, counter: Counter) =
    if (isRisk) {
      val id = MetricID(name, Category.Counter(CounterKind.Risk), token.hash).identifier
      (id, metricRegistry.counter(id))
    } else {
      val id = MetricID(name, Category.Counter(CounterKind.Counter), token.hash).identifier
      (id, metricRegistry.counter(id))
    }

  def unsafeInc(num: Long): Unit = counter.inc(num)
  def unsafeDec(num: Long): Unit = counter.dec(num)

  def inc(num: Long): F[Unit] = F.delay(counter.inc(num))
  def dec(num: Long): F[Unit] = F.delay(counter.dec(num))
  def getCount: F[Long]       = F.delay(counter.getCount)

  def kleisli[A](f: A => Long): Kleisli[F, A, Unit] = Kleisli(inc).local(f)

  private val unregister: F[Unit] = F.delay {
    metricRegistry.remove(counter_name)
  }.void

}

object NJCounter {

  final class Builder private[guard] (measurement: Measurement, isRisk: Boolean) {

    def withMeasurement(measurement: String): Builder = new Builder(Measurement(measurement), isRisk)

    def asRisk: Builder = new Builder(measurement, true)

    private[guard] def build[F[_]: Sync](
      name: String,
      metricRegistry: MetricRegistry,
      serviceParams: ServiceParams): Resource[F, NJCounter[F]] = {
      val metricName = MetricName(serviceParams, measurement, name)
      Resource.make(Sync[F].unique.map(new NJCounter[F](_, metricName, metricRegistry, isRisk)))(_.unregister)
    }
  }
}
