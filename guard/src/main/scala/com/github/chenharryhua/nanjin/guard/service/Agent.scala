package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.{Console, Dispatcher}
import com.codahale.metrics.MetricRegistry
import com.github.benmanes.caffeine.cache.Cache
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.resilience.{CircuitBreaker, Retry}
import com.github.chenharryhua.nanjin.guard.batch.Batch
import com.github.chenharryhua.nanjin.guard.cache.CaffeineCache
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import logging.{Log, LogSink, Logger}
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub
import fs2.Stream
import fs2.concurrent.Channel
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
  def herald(f: AlarmLevel.type => AlarmLevel): Resource[F, Log[F]]
  def logger(using ln: LoggerName): Resource[F, Log[F]]

  /*
   * metrics
   */
  def metricsHub(label: String): MetricsHub[F]
  def facilitate[A](label: String)(f: MetricsHub[F] => A): A
  def adhoc: AdhocMetrics[F]

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

final private class GeneralAgent[F[_]: {Async, Console}](
  serviceParams: ServiceParams,
  domain: Domain,
  metricRegistry: MetricRegistry,
  channel: Channel[F, Event],
  errorHistory: History[F, ReportedEvent],
  dispatcher: Dispatcher[F],
  uuidGenerator: F[UUID],
  alarmLevel: Ref[F, Option[AlarmLevel]],
  adhocMetrics: AdhocMetrics[F])
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withDomain(name: String): Agent[F] =
    new GeneralAgent[F](
      serviceParams = serviceParams,
      domain = Domain(name),
      metricRegistry = metricRegistry,
      channel = channel,
      errorHistory = errorHistory,
      dispatcher = dispatcher,
      uuidGenerator = uuidGenerator,
      alarmLevel = alarmLevel,
      adhocMetrics = adhocMetrics
    )

  override def tickScheduled(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickScheduled[F](zoneId, f)

  override def tickFuture(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickFuture[F](zoneId, f)

  override def tickImmediate(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickImmediate[F](zoneId, f)

  override def metricsHub(label: String): MetricsHub[F] = {
    val metricLabel = MetricLabel(label, domain)
    new MetricsHub.Impl[F](metricLabel, metricRegistry, dispatcher, zoneId)
  }

  override def batch(label: String): Batch[F] = new Batch[F](metricsHub(label), uuidGenerator)

  override def facilitate[A](label: String)(f: MetricsHub[F] => A): A =
    f(metricsHub(label))

  override def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]] =
    CircuitBreaker[F](zoneId, f)

  override def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]] =
    CaffeineCache[F, K, V](cache)

  override def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]] =
    Resource.eval(Retry[F](zoneId, f))

  override val adhoc: AdhocMetrics[F] = adhocMetrics

  override def herald(f: AlarmLevel.type => AlarmLevel): Resource[F, Log[F]] =
    Resource.pure(
      Herald(
        serviceParams = serviceParams,
        domain = domain,
        alarmLevel = alarmLevel,
        alarmThreshold = f(AlarmLevel),
        channel = channel,
        errorHistory = errorHistory))

  override def logger(using ln: LoggerName): Resource[F, Log[F]] =
    Resource
      .eval(LogSink(serviceParams.logFormat, zoneId, ln))
      .map(logSink =>
        Logger(serviceParams = serviceParams, domain = domain, alarmLevel = alarmLevel, logSink = logSink))
}
