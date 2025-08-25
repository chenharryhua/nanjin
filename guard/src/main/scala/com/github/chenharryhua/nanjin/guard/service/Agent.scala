package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{AtomicCell, Dispatcher}
import cats.implicits.catsSyntaxApplicativeId
import com.codahale.metrics.MetricRegistry
import com.github.benmanes.caffeine.cache.Cache
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.{CaffeineCache, CircuitBreaker, Retry}
import com.github.chenharryhua.nanjin.guard.batch.{Batch, LightBatch}
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue

import java.time.ZoneId

sealed trait Agent[F[_]] {
  def zoneId: ZoneId

  def withDomain(name: String): Agent[F]

  /*
   * batch
   */
  def batch(label: String): Batch[F]
  def lightBatch(label: String): LightBatch[F]

  def ticks(policy: Policy): Stream[F, Tick]
  final def ticks(f: Policy.type => Policy): Stream[F, Tick] =
    ticks(f(Policy))

  /** start ticking immediately
    */
  def tickImmediately(policy: Policy): Stream[F, Tick]
  final def tickImmediately(f: Policy.type => Policy): Stream[F, Tick] =
    tickImmediately(f(Policy))

  /*
   * metrics adhoc report/reset
   */
  val adhoc: AdhocMetrics[F]

  /*
   * Service Message
   */
  val herald: Herald[F]
  val log: Log[F]

  def facilitate[A](label: String)(f: Metrics[F] => A): A

  def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]]

  def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]]

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

  override lazy val zoneId: ZoneId = eventLogger.serviceParams.zoneId

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

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream.past[F](zoneId, policy)

  override def tickImmediately(policy: Policy): Stream[F, Tick] =
    tickStream.future(zoneId, policy)

  override def facilitate[A](label: String)(f: Metrics[F] => A): A = {
    val metricLabel = MetricLabel(label, domain)
    f(new Metrics.Impl[F](metricLabel, metricRegistry, dispatcher, zoneId))
  }

  override def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]] =
    f(new CircuitBreaker.Builder(maxFailures = 5, policy = Policy.giveUp)).build[F](zoneId)

  override def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]] =
    CaffeineCache.build[F, K, V](cache)

  override def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]] =
    f(new Retry.Builder[F](Policy.giveUp, _ => true.pure[F])).build(zoneId)

  override object adhoc extends AdhocMetricsImpl[F](channel, eventLogger, metricRegistry)

  override object herald extends HeraldImpl[F](channel, eventLogger, errorHistory)
  override val log: Log[F] = eventLogger
}
