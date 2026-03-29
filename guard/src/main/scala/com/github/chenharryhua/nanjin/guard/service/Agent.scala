package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Console, Dispatcher}
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.common.resilience.{CircuitBreaker, Retry}
import com.github.chenharryhua.nanjin.guard.batch.Batch
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.metrics.MetricsHub
import com.github.chenharryhua.nanjin.guard.service.logging.Log
import fs2.Stream
import fs2.concurrent.Channel

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

  val herald: Log[F]
  val logger: Log[F]
  val heraldLogger: Log[F]

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
   * retry
   */
  def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]]

}

final private class GeneralAgent[F[_]: {Async, Console}](
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  dispatcher: Dispatcher[F],
  uuidGenerator: F[UUID],
  metricsEventHandler: MetricsEventHandler[F],
  reportedEventHandler: ReportedEventHandler[F])
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withDomain(name: String): Agent[F] =
    new GeneralAgent[F](
      serviceParams = serviceParams,
      channel = channel,
      dispatcher = dispatcher,
      uuidGenerator = uuidGenerator,
      metricsEventHandler = metricsEventHandler,
      reportedEventHandler = reportedEventHandler.withDomain(name)
    )

  override def tickScheduled(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickScheduled[F](zoneId, f)

  override def tickFuture(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickFuture[F](zoneId, f)

  override def tickImmediate(f: Policy.type => Policy): Stream[F, Tick] =
    tickStream.tickImmediate[F](zoneId, f)

  override def metricsHub(label: String): MetricsHub[F] = {
    val metricLabel = MetricLabel(label, reportedEventHandler.domain)
    new MetricsHub.Impl[F](metricLabel, metricsEventHandler.scrapeMetrics.metricRegistry, dispatcher, zoneId)
  }

  override def batch(label: String): Batch[F] = new Batch[F](metricsHub(label), uuidGenerator)

  override def facilitate[A](label: String)(f: MetricsHub[F] => A): A =
    f(metricsHub(label))

  override def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]] =
    CircuitBreaker[F](zoneId, f)

  override def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]] =
    Resource.eval(Retry[F](zoneId, f))

  override val adhoc: AdhocMetrics[F] = metricsEventHandler

  override val herald: Log[F] = reportedEventHandler.herald

  override val logger: Log[F] = reportedEventHandler.logger

  override val heraldLogger: Log[F] = reportedEventHandler.heraldLogger

}
