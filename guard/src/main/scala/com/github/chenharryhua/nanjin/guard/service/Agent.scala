package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{AtomicCell, Console, Dispatcher}
import com.codahale.metrics.MetricRegistry
import com.github.benmanes.caffeine.cache.Cache
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.{CircuitBreaker, Retry}
import com.github.chenharryhua.nanjin.guard.batch.Batch
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import com.github.chenharryhua.nanjin.guard.logging.{Herald, Log, LogEvent, Logger}
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.typelevel.log4cats.LoggerName

import java.time.ZoneId
import java.util.UUID

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

  /*
   * ticks
   */
  def tickScheduled(f: Policy.type => Policy): Stream[F, Tick]
  def tickImmediate(f: Policy.type => Policy): Stream[F, Tick]
  def tickFuture(f: Policy.type => Policy): Stream[F, Tick]

  /*
   * Service Message
   */
  def herald: Resource[F, Log[F]]
  def logger(implicit ln: LoggerName): Resource[F, Log[F]]

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

final private class GeneralAgent[F[_]: Async: Console](
  serviceParams: ServiceParams,
  domain: Domain,
  metricRegistry: MetricRegistry,
  channel: Channel[F, Event],
  errorHistory: AtomicCell[F, CircularFifoQueue[ServiceMessage]],
  dispatcher: Dispatcher[F],
  uuidGenerator: F[UUID],
  logEvent: LogEvent[F],
  alarmLevel: Ref[F, Option[AlarmLevel]])
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withDomain(name: String): Agent[F] =
    new GeneralAgent[F](
      serviceParams,
      Domain(name),
      metricRegistry,
      channel,
      errorHistory,
      dispatcher,
      uuidGenerator,
      logEvent,
      alarmLevel)

  override def batch(label: String): Batch[F] = {
    val metricLabel = MetricLabel(label, domain)
    new Batch[F](new Metrics.Impl[F](metricLabel, metricRegistry, dispatcher, zoneId), uuidGenerator)
  }

  override def tickScheduled(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickScheduled[F](zoneId, f)

  override def tickFuture(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickFuture[F](zoneId, f)

  override def tickImmediate(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickImmediate[F](zoneId, f)

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

  override object adhoc extends AdhocMetricsImpl[F](serviceParams, channel, logEvent, metricRegistry)

  override def herald: Resource[F, Log[F]] =
    Resource.pure(
      Herald(serviceParams = serviceParams, domain = domain, channel = channel, errorHistory = errorHistory))

  override def logger(implicit ln: LoggerName): Resource[F, Log[F]] =
    Resource
      .eval(LogEvent(serviceParams.logFormat, zoneId, ln))
      .map(logEvent =>
        Logger(serviceParams = serviceParams, domain = domain, alarmLevel = alarmLevel, logEvent = logEvent))

}
