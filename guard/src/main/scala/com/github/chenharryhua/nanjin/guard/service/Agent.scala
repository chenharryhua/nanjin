package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{AtomicCell, Dispatcher}
import com.codahale.metrics.MetricRegistry
import com.github.benmanes.caffeine.cache.Cache
import com.github.chenharryhua.nanjin.common.{CircuitBreaker, Retry}
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.batch.{Batch, LightBatch}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue

import java.time.ZoneId

/** Agent is the primary service façade providing scoped access to metrics, batching, scheduling, and
  * resilience primitives.
  */
sealed trait Agent[F[_]] {
  val zoneId: ZoneId

  def withDomain(name: String): Agent[F]

  /*
   * batch
   */
  def batch(label: String): Batch[F]
  def lightBatch(label: String): LightBatch[F]

  /*
   * ticks
   */
  def tickScheduled(policy: Policy): Stream[F, Tick]
  def tickScheduled(f: Policy.type => Policy): Stream[F, Tick]

  def tickImmediate(policy: Policy): Stream[F, Tick]
  def tickImmediate(f: Policy.type => Policy): Stream[F, Tick]

  def tickFuture(policy: Policy): Stream[F, Tick]
  def tickFuture(f: Policy.type => Policy): Stream[F, Tick]

  /*
   * Service Message
   */
  val herald: Herald[F]
  val log: Log[F]

  /*
   * metrics
   */
  def metrics(label: String): Metrics[F]
  def facilitate[A](label: String)(f: Metrics[F] => A): A
  val adhoc: AdhocMetrics[F]

  /*
   * circuit breaker
   */
  def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]]

  /*
   * cache
   */
  def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]]

  /*
   * retry
   */
  def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]]

}

final private class GeneralAgent[F[_]: Async](
  metricRegistry: MetricRegistry,
  channel: Channel[F, Event],
  eventLogger: EventLogger[F],
  domain: Domain,
  alarmLevel: Ref[F, Option[AlarmLevel]],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]],
  dispatcher: Dispatcher[F])
    extends Agent[F] {

  override val zoneId: ZoneId = eventLogger.serviceParams.zoneId

  override def withDomain(name: String): Agent[F] =
    new GeneralAgent[F](
      metricRegistry,
      channel,
      eventLogger,
      Domain(name),
      alarmLevel,
      errorHistory,
      dispatcher)

  override def batch(label: String): Batch[F] = {
    val metricLabel = MetricLabel(label, domain)
    new Batch[F](new Metrics.Impl[F](metricLabel, metricRegistry, dispatcher, zoneId))
  }
  override def lightBatch(label: String): LightBatch[F] = {
    val metricLabel = MetricLabel(label, domain)
    new LightBatch[F](new Metrics.Impl[F](metricLabel, metricRegistry, dispatcher, zoneId))
  }

  override def tickScheduled(policy: Policy): Stream[F, Tick] =
    tickStream.tickScheduled[F](zoneId, policy)

  override def tickScheduled(f: Policy.type => Policy): Stream[F, Tick] =
    tickScheduled(f(Policy))

  override def tickFuture(policy: Policy): Stream[F, Tick] =
    tickStream.tickFuture[F](zoneId, policy)

  override def tickFuture(f: Policy.type => Policy): Stream[F, Tick] =
    tickFuture(f(Policy))

  override def tickImmediate(policy: Policy): Stream[F, Tick] =
    tickStream.tickImmediate[F](zoneId, policy)

  override def tickImmediate(f: Policy.type => Policy): Stream[F, Tick] =
    tickImmediate(f(Policy))

  override def metrics(label: String): Metrics[F] = {
    val metricLabel = MetricLabel(label, domain)
    new Metrics.Impl[F](metricLabel, metricRegistry, dispatcher, zoneId)
  }

  override def facilitate[A](label: String)(f: Metrics[F] => A): A =
    f(metrics(label))

  override def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]] =
    CircuitBreaker[F](zoneId, f)

  override def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]] =
    CaffeineCache[F, K, V](cache)

  override def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]] =
    Resource.eval(Retry[F](zoneId, f))

  override object adhoc extends AdhocMetricsImpl[F](channel, eventLogger, metricRegistry)

  override object herald extends HeraldImpl[F](channel, eventLogger, errorHistory)
  override val log: Log[F] = eventLogger
}
