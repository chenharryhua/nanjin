package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, Console}
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import fs2.Stream
import org.apache.commons.collections4.queue.CircularFifoQueue
import org.typelevel.log4cats.LoggerName

final private class ServiceBuildHelper[F[_]: Async](serviceParams: ServiceParams) {
  private val F = Async[F]

  def error_history: Stream[F, AtomicCell[F, CircularFifoQueue[ReportedEvent]]] =
    Stream.eval(AtomicCell[F].of(new CircularFifoQueue[ReportedEvent](serviceParams.historyCapacity.error)))

  def service_log_sink(implicit F: Console[F]): Stream[F, LogSink[F]] =
    Stream.eval(
      LogSink[F](serviceParams.logFormat, serviceParams.zoneId, LoggerName(serviceParams.serviceName.value)))

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
