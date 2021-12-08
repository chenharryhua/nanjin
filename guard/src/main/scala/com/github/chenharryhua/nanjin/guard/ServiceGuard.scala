package com.github.chenharryhua.nanjin.guard

import cats.data.Reader
import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.concurrent.Channel
import fs2.{INothing, Stream}
// format: off
/** @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name") 
  *       val es: Stream[IO,NJEvent] = guard.eventStream {
  *           gd => gd("action-1").retry(IO(1)).run >> 
  *                  IO("other computation") >> 
  *                  gd("action-2").retry(IO(2)).run 
  *            }
  * }}}
  */
// format: on

final class ServiceGuard[F[_]] private[guard] (
  serviceConfig: ServiceConfig,
  metricFilter: MetricFilter,
  jmxBuilder: Option[Reader[JmxReporter.Builder, JmxReporter.Builder]])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  val params: ServiceParams = serviceConfig.evalConfig

  override def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig), metricFilter, jmxBuilder)

  def apply(serviceName: String): ServiceGuard[F] = updateConfig(_.withServiceName(serviceName))

  def withJmxReporter(builder: JmxReporter.Builder => JmxReporter.Builder): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricFilter, Some(Reader(builder)))

  def withMetricFilter(filter: MetricFilter) =
    new ServiceGuard[F](serviceConfig, filter, jmxBuilder)

  def eventStream[A](actionGuard: ActionGuard[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceInfo <- Stream.eval(F.realTimeInstant.map(_.atZone(params.taskParams.zoneId)).map(ServiceInfo))
      event <- Stream.eval(Channel.bounded[F, NJEvent](params.queueCapacity)).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()
        val publisher: EventPublisher[F]   = new EventPublisher[F](serviceInfo, metricRegistry, channel, params)

        val theService: F[A] = retry.mtl
          .retryingOnAllErrors(params.retry.policy[F], (ex: Throwable, rd) => publisher.servicePanic(rd, ex)) {
            publisher.serviceReStarted *> Dispatcher[F].use(dispatcher =>
              actionGuard(new ActionGuard[F](publisher, dispatcher, ActionConfig(params))))
          }
          .guarantee(publisher.serviceStopped(metricFilter) <* channel.close)

        /** concurrent streams
          */
        val cronScheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))

        val metricsReport: Stream[F, INothing] = {
          params.metric.reportSchedule match {
            case Left(dur) =>
              Stream
                .fixedRate[F](dur)
                .zipWithIndex
                .evalMap(t => publisher.metricsReport(metricFilter, t._2 + 1, dur))
                .drain
            case Right(cron) =>
              cronScheduler
                .awakeEvery(cron)
                .zipWithIndex
                .evalMap(t => publisher.metricsReport(metricFilter, t._2 + 1, cron))
                .drain
          }
        }

        val metricsReset: Stream[F, INothing] = params.metric.resetSchedule.fold(Stream.empty.covary[F])(cron =>
          cronScheduler.awakeEvery(cron).evalMap(_ => publisher.metricsReset(metricFilter, cron)).drain)

        val jmxReporting: Stream[F, INothing] = {
          jmxBuilder match {
            case None => Stream.empty
            case Some(builder) =>
              Stream
                .bracket(F.delay(builder.run(JmxReporter.forRegistry(metricRegistry)).filter(metricFilter).build()))(
                  r => F.delay(r.close()))
                .evalMap(jr => F.delay(jr.start()))
                .flatMap(_ => Stream.never[F])
          }
        }

        // put together

        channel.stream
          .onFinalize(channel.close.void) // drain pending send operation
          .concurrently(Stream.eval(theService).drain)
          .concurrently(metricsReport)
          .concurrently(metricsReset)
          .concurrently(jmxReporting)
      }
    } yield event
}
