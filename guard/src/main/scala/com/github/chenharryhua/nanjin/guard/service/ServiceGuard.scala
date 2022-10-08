package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Console, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.Endo
import com.codahale.metrics.{MetricFilter, MetricRegistry, MetricSet}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.config.{ScheduleType, ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{MetricReport, MetricReset}
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cron4s.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.Stream
import fs2.concurrent.Channel
import natchez.EntryPoint
import retry.RetryDetails

import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.*
import scala.util.control.NonFatal

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
  metricSet: List[MetricSet],
  metricFilter: MetricFilter,
  jmxBuilder: Option[Endo[JmxReporter.Builder]],
  entryPoint: Resource[F, EntryPoint[F]])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig), metricSet, metricFilter, jmxBuilder, entryPoint)

  def apply(serviceName: ServiceName): ServiceGuard[F] =
    updateConfig(_.withServiceName(serviceName))

  def withJmxReporter(builder: Endo[JmxReporter.Builder]): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, metricFilter, Some(builder), entryPoint)

  def withMetricFilter(filter: MetricFilter): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, filter, jmxBuilder, entryPoint)

  def addMetricSet(ms: MetricSet): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, ms :: metricSet, metricFilter, jmxBuilder, entryPoint)

  private val initStatus: F[ServiceParams] = for {
    uuid <- UUIDGen.randomUUID
    ts <- F.realTimeInstant
  } yield serviceConfig.evalConfig(uuid, ts)

  def dummyAgent(implicit C: Console[F]): Resource[F, Agent[F]] = for {
    sp <- Resource.eval(initStatus)
    chn <- Resource.eval(Channel.bounded[F, NJEvent](0))
    _ <- chn.stream
      .evalMap(evt => Translator.simpleText[F].translate(evt).flatMap(_.traverse(C.println)))
      .compile
      .resource
      .drain
  } yield new Agent[F](new MetricRegistry, chn, sp, entryPoint)

  def eventStream[A](runAgent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceParams <- Stream.eval(initStatus)
      event <- Stream.eval(Channel.bounded[F, NJEvent](serviceParams.queueCapacity.value)).flatMap {
        channel =>
          val metricRegistry: MetricRegistry = {
            val mr = new MetricRegistry()
            metricSet.foreach(mr.registerAll)
            mr
          }

          val runningService: Stream[F, Nothing] = {
            val agent = new Agent[F](metricRegistry, channel, serviceParams, entryPoint)
            Stream
              .eval[F, A] {
                retry.mtl
                  .retryingOnSomeErrors[A](
                    serviceParams.retry.policy[F],
                    (ex: Throwable) => F.pure(NonFatal(ex)), // give up on fatal errors
                    (ex: Throwable, rd) =>
                      rd match {
                        case RetryDetails.GivingUp(_, _) => F.unit
                        case RetryDetails.WillDelayAndRetry(nextDelay, _, _) =>
                          serviceEventPublisher.servicePanic(channel, serviceParams, nextDelay, ex)
                      }
                  )(serviceEventPublisher.serviceReStart(channel, serviceParams) *> runAgent(agent))
                  .guaranteeCase(oc =>
                    serviceEventPublisher
                      .serviceStop(channel, serviceParams, ServiceStopCause(oc)) <* channel.close)
              }
              .onFinalize(F.sleep(1.second))
              .drain
          }

          // concurrent streams

          val cronScheduler: Scheduler[F, CronExpr] =
            Cron4sScheduler.from(F.pure(serviceParams.taskParams.zoneId))

          val metricsReport: Stream[F, MetricReport] =
            serviceParams.metric.reportSchedule match {
              case Some(ScheduleType.Fixed(dur)) =>
                // https://stackoverflow.com/questions/24649842/scheduleatfixedrate-vs-schedulewithfixeddelay
                Stream
                  .fixedRate[F](dur.toScala)
                  .zipWithIndex
                  .evalMap(t =>
                    serviceEventPublisher.metricReport(
                      serviceParams,
                      metricRegistry,
                      metricFilter,
                      MetricReportType.Scheduled(t._2)))
              case Some(ScheduleType.Cron(cron)) =>
                cronScheduler
                  .awakeEvery(cron)
                  .zipWithIndex
                  .evalMap(t =>
                    serviceEventPublisher.metricReport(
                      serviceParams,
                      metricRegistry,
                      metricFilter,
                      MetricReportType.Scheduled(t._2)))
              case None => Stream.empty
            }

          val metricsReset: Stream[F, MetricReset] =
            serviceParams.metric.resetSchedule match {
              case None => Stream.empty
              case Some(cron) =>
                cronScheduler
                  .awakeEvery(cron)
                  .evalMap(_ => serviceEventPublisher.metricReset(serviceParams, metricRegistry, Some(cron)))
            }

          val jmxReporting: Stream[F, Nothing] =
            jmxBuilder match {
              case None => Stream.empty
              case Some(builder) =>
                Stream
                  .bracket(
                    F.delay(builder(JmxReporter.forRegistry(metricRegistry)).filter(metricFilter).build()))(
                    r => F.delay(r.close()))
                  .evalMap(jr => F.delay(jr.start()))
                  .flatMap(_ => Stream.never[F])
            }

          // put together

          channel.stream
            .onFinalize(channel.close.void) // drain pending send operation
            .mergeHaltL(metricsReset)
            .mergeHaltL(metricsReport)
            .concurrently(runningService)
            .concurrently(jmxReporting)
      }
    } yield event
}
