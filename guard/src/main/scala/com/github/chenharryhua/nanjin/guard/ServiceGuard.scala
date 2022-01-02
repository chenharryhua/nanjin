package com.github.chenharryhua.nanjin.guard

import cats.data.Reader
import cats.effect.kernel.Async
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{Counter, MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.{servicePanicMRName, serviceRestartMRName}
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

// https://github.com/dropwizard/metrics
final class ServiceGuard[F[_]] private[guard] (
  serviceConfig: ServiceConfig,
  metricFilter: MetricFilter,
  jmxBuilder: Option[Reader[JmxReporter.Builder, JmxReporter.Builder]])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  val serviceParams: ServiceParams = serviceConfig.evalConfig

  override def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig), metricFilter, jmxBuilder)

  def apply(serviceName: String): ServiceGuard[F] = updateConfig(_.withServiceName(serviceName))

  def withJmxReporter(builder: JmxReporter.Builder => JmxReporter.Builder): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricFilter, Some(Reader(builder)))

  def withMetricFilter(filter: MetricFilter) =
    new ServiceGuard[F](serviceConfig, filter, jmxBuilder)

  def withBrief(brief: String): ServiceGuard[F] = updateConfig(_.withBrief(brief))

  def eventStream[A](agent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceStatus <- Stream.eval(for {
        uuid <- UUIDGen.randomUUID
        ts <- F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))
        ssRef <- F.ref(ServiceStatus.Up(uuid, ts))
      } yield ssRef)
      lastCountersRef <- Stream.eval(F.ref(MetricSnapshot.LastCounters.empty))
      ongoingCriticalActions <- Stream.eval(F.ref(Set.empty[ActionInfo]))
      event <- Stream.eval(Channel.bounded[F, NJEvent](serviceParams.queueCapacity)).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()
        val publisher: EventPublisher[F] =
          new EventPublisher[F](
            serviceParams,
            metricRegistry,
            ongoingCriticalActions,
            serviceStatus,
            lastCountersRef,
            channel)

        val panicCounter: Counter   = publisher.metricRegistry.counter(servicePanicMRName)
        val restartCounter: Counter = publisher.metricRegistry.counter(serviceRestartMRName)

        val theService: F[A] = retry.mtl
          .retryingOnAllErrors(
            serviceParams.retry.policy[F],
            (ex: Throwable, rd) => publisher.servicePanic(rd, ex).map(_ => panicCounter.inc(1))) {
            publisher.serviceReStarted.map(_ => restartCounter.inc(1)) *> Dispatcher[F].use(dispatcher =>
              agent(new Agent[F](publisher, dispatcher, AgentConfig())))
          }
          .guarantee(publisher.serviceStopped <* channel.close)

        /** concurrent streams
          */
        val cronScheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(serviceParams.taskParams.zoneId))

        val metricsReport: Stream[F, INothing] = {
          serviceParams.metric.reportSchedule match {
            case Some(Left(dur)) =>
              // https://stackoverflow.com/questions/24649842/scheduleatfixedrate-vs-schedulewithfixeddelay
              Stream
                .fixedRate[F](dur)
                .zipWithIndex
                .evalMap(t =>
                  publisher
                    .metricsReport(metricFilter, MetricReportType.Scheduled(serviceParams.metric.snapshotType, t._2)))
                .drain
            case Some(Right(cron)) =>
              cronScheduler
                .awakeEvery(cron)
                .zipWithIndex
                .evalMap(t =>
                  publisher
                    .metricsReport(metricFilter, MetricReportType.Scheduled(serviceParams.metric.snapshotType, t._2)))
                .drain
            case None => Stream.empty
          }
        }

        val metricsReset: Stream[F, INothing] = serviceParams.metric.resetSchedule.fold(Stream.empty.covary[F])(cron =>
          cronScheduler.awakeEvery(cron).evalMap(_ => publisher.metricsReset(Some(cron))).drain)

        val jmxReporting: Stream[F, INothing] =
          jmxBuilder match {
            case None => Stream.empty
            case Some(builder) =>
              Stream
                .bracket(F.delay(builder.run(JmxReporter.forRegistry(metricRegistry)).filter(metricFilter).build()))(
                  r => F.delay(r.close()))
                .evalMap(jr => F.delay(jr.start()))
                .flatMap(_ => Stream.never[F])
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
