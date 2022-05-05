package com.github.chenharryhua.nanjin.guard.service

import cats.data.Reader
import cats.effect.kernel.{Async, Outcome, Ref}
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.jmx.JmxReporter
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{AgentConfig, ServiceConfig, ServiceName, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.concurrent.Channel
import fs2.{INothing, Stream}
import org.apache.commons.lang3.exception.ExceptionUtils

// format: off
/** @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name") 
  *       val es: Stream[IO,NJEvent] = guard.eventStream {
  *           gd => gd.span("action-1").retry(IO(1)).run >> 
  *                  IO("other computation") >> 
  *                  gd.span("action-2").retry(IO(2)).run 
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

  def apply(serviceName: ServiceName): ServiceGuard[F] = updateConfig(_.withServiceName(serviceName))

  def withJmxReporter(builder: JmxReporter.Builder => JmxReporter.Builder): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricFilter, Some(Reader(builder)))

  def withMetricFilter(filter: MetricFilter) =
    new ServiceGuard[F](serviceConfig, filter, jmxBuilder)

  private val initStatus: F[Ref[F, ServiceStatus]] = for {
    uuid <- UUIDGen.randomUUID
    ts <- F.realTimeInstant.map(serviceParams.toZonedDateTime)
    ssRef <- F.ref(ServiceStatus.Up(serviceParams, uuid, ts))
  } yield ssRef

  def eventStream[A](runAgent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceStatus <- Stream.eval(initStatus)
      lastCounters <- Stream.eval(F.ref(MetricSnapshot.LastCounters.empty))
      ongoings <- Stream.eval(F.ref(Set.empty[ActionInfo])) // currently running actions
      event <- Stream.eval(Channel.bounded[F, NJEvent](serviceParams.queueCapacity.value)).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()

        val theService: F[A] = {
          val sep: ServiceEventPublisher[F] = new ServiceEventPublisher[F](serviceStatus, channel)

          retry.mtl
            .retryingOnAllErrors(serviceParams.retry.policy[F], (ex: Throwable, rd) => sep.servicePanic(rd, ex)) {
              sep.serviceReStart *> Dispatcher[F].use(dispatcher =>
                runAgent(
                  new Agent[F](
                    metricRegistry,
                    serviceStatus,
                    channel,
                    ongoings,
                    dispatcher,
                    lastCounters,
                    AgentConfig(serviceParams))))
            }
            .guaranteeCase {
              case Outcome.Succeeded(_) =>
                sep.serviceStop(ServiceStopCause.Normally) <* channel.close
              case Outcome.Errored(e) =>
                sep.serviceStop(ServiceStopCause.Abnormally(ExceptionUtils.getMessage(e))) <* channel.close
              case Outcome.Canceled() =>
                sep.serviceStop(ServiceStopCause.Abnormally("canceled")) <* channel.close
            }
        }

        /** concurrent streams
          */
        val cronScheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(serviceParams.taskParams.zoneId))

        val metricEventPublisher: MetricEventPublisher[F] =
          new MetricEventPublisher[F](channel, metricRegistry, serviceStatus, ongoings, lastCounters)

        val metricsReport: Stream[F, INothing] =
          serviceParams.metric.reportSchedule match {
            case Some(Left(dur)) =>
              // https://stackoverflow.com/questions/24649842/scheduleatfixedrate-vs-schedulewithfixeddelay
              Stream
                .fixedRate[F](dur)
                .zipWithIndex
                .evalMap(t =>
                  metricEventPublisher
                    .metricsReport(metricFilter, MetricReportType.Scheduled(serviceParams.metric.snapshotType, t._2)))
                .drain
            case Some(Right(cron)) =>
              cronScheduler
                .awakeEvery(cron)
                .zipWithIndex
                .evalMap(t =>
                  metricEventPublisher
                    .metricsReport(metricFilter, MetricReportType.Scheduled(serviceParams.metric.snapshotType, t._2)))
                .drain
            case None => Stream.empty
          }

        val metricsReset: Stream[F, INothing] = serviceParams.metric.resetSchedule.fold(Stream.empty.covary[F])(cron =>
          cronScheduler.awakeEvery(cron).evalMap(_ => metricEventPublisher.metricsReset(Some(cron))).drain)

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
