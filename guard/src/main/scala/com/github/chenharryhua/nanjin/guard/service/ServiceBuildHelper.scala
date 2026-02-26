package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, Console}
import cats.syntax.flatMap.toFlatMapOps
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Policy, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event
import com.github.chenharryhua.nanjin.guard.event.Event.{MetricsSnapshot, ReportedEvent, ServicePanic}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.Periodic
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.typelevel.log4cats.LoggerName

final private class ServiceBuildHelper[F[_]: Async](serviceParams: ServiceParams) {
  private val F = Async[F]

  def panic_history: Stream[F, AtomicCell[F, CircularFifoQueue[ServicePanic]]] =
    Stream.eval(AtomicCell[F].of(new CircularFifoQueue[ServicePanic](serviceParams.historyCapacity.panic)))

  def metrics_history: Stream[F, AtomicCell[F, CircularFifoQueue[MetricsSnapshot]]] =
    Stream.eval(
      AtomicCell[F].of(new CircularFifoQueue[MetricsSnapshot](serviceParams.historyCapacity.metric)))

  def error_history: Stream[F, AtomicCell[F, CircularFifoQueue[ReportedEvent]]] =
    Stream.eval(AtomicCell[F].of(new CircularFifoQueue[ReportedEvent](serviceParams.historyCapacity.error)))

  def log_sink(implicit F: Console[F]): Stream[F, LogSink[F]] =
    Stream.eval(
      LogSink[F](serviceParams.logFormat, serviceParams.zoneId, LoggerName(serviceParams.serviceName.value)))

  private def tickingBy(policy: Policy): Stream[F, Tick] =
    tickStream.tickScheduled(serviceParams.zoneId, _ => policy)

  def service_metrics_report(
    channel: Channel[F, Event],
    logSink: LogSink[F],
    metricRegistry: MetricRegistry,
    metricsHistory: AtomicCell[F, CircularFifoQueue[Event.MetricsSnapshot]]): Stream[F, Nothing] =
    tickingBy(serviceParams.servicePolicies.metricsReport).evalMap { tick =>
      publish_metrics_report(
        serviceParams = serviceParams,
        channel = channel,
        logSink = logSink,
        metricRegistry = metricRegistry,
        index = Periodic(tick)).flatMap(mr => metricsHistory.modify(queue => (queue, queue.add(mr))))
    }.drain

  def service_metrics_reset(
    channel: Channel[F, Event],
    logSink: LogSink[F],
    metricRegistry: MetricRegistry): Stream[F, Nothing] =
    tickingBy(serviceParams.servicePolicies.metricsReset)
      .evalMap(tick =>
        publish_metrics_reset(
          serviceParams = serviceParams,
          channel = channel,
          logSink = logSink,
          metricRegistry = metricRegistry,
          index = Periodic(tick)))
      .drain

  def service_jmx_report(
    metricRegistry: MetricRegistry,
    jmxBuilder: Option[Endo[JmxReporter.Builder]]): Stream[F, Nothing] =
    jmxBuilder match {
      case None        => Stream.empty
      case Some(build) =>
        Stream.bracket(F.blocking {
          val reporter =
            build(JmxReporter.forRegistry(metricRegistry)) // use home-brew factory
              .createsObjectNamesWith(objectNameFactory)
              .build()
          reporter.start()
          reporter
        })(r => F.blocking(r.stop())) >>
          Stream.never[F]
    }
}
