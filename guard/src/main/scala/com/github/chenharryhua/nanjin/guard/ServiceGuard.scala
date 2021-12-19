package com.github.chenharryhua.nanjin.guard

import cats.data.Reader
import cats.effect.kernel.Async
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{AgentConfig, ServiceConfig, ServiceParams}
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

  def eventStream[A](agent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceInfo <- Stream.eval(for {
        uuid <- UUIDGen.randomUUID
        ts <- F.realTimeInstant.map(_.atZone(params.taskParams.zoneId))
      } yield ServiceInfo(params, uuid, ts))
      event <- Stream.eval(Channel.bounded[F, NJEvent](params.queueCapacity)).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()
        val publisher: EventPublisher[F]   = new EventPublisher[F](serviceInfo, metricRegistry, channel)

        val theService: F[A] = retry.mtl
          .retryingOnAllErrors(params.retry.policy[F], (ex: Throwable, rd) => publisher.servicePanic(rd, ex)) {
            publisher.serviceReStarted *> Dispatcher[F].use(dispatcher =>
              agent(new Agent[F](publisher, dispatcher, AgentConfig())))
          }
          .guarantee(publisher.serviceStopped(metricFilter) <* channel.close)

        /** concurrent streams
          */
        val cronScheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))

        val metricsReport: Stream[F, INothing] = {
          params.metric.reportSchedule match {
            case Some(Left(dur)) =>
              // https://stackoverflow.com/questions/24649842/scheduleatfixedrate-vs-schedulewithfixeddelay
              Stream
                .fixedRate[F](dur)
                .zipWithIndex
                .evalMap(t => publisher.metricsReport(metricFilter, MetricReportType.ScheduledReport(t._2 + 1)))
                .drain
            case Some(Right(cron)) =>
              cronScheduler
                .awakeEvery(cron)
                .zipWithIndex
                .evalMap(t => publisher.metricsReport(metricFilter, MetricReportType.ScheduledReport(t._2 + 1)))
                .drain
            case None => Stream.empty
          }
        }

        val metricsReset: Stream[F, INothing] = params.metric.resetSchedule.fold(Stream.empty.covary[F])(cron =>
          cronScheduler.awakeEvery(cron).evalMap(_ => publisher.metricsReset(metricFilter, Some(cron))).drain)

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
