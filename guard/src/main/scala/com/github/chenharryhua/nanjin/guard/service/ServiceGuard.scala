package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Console, UUIDGen}
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
import retry.RetryPolicy

import scala.jdk.DurationConverters.*

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
  entryPoint: Resource[F, EntryPoint[F]],
  retryPolicy: RetryPolicy[F])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig), metricSet, metricFilter, jmxBuilder, entryPoint, retryPolicy)

  def apply(serviceName: ServiceName): ServiceGuard[F] =
    updateConfig(_.withServiceName(serviceName))

  def withJmxReporter(builder: Endo[JmxReporter.Builder]): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, metricFilter, Some(builder), entryPoint, retryPolicy)

  def withMetricFilter(filter: MetricFilter): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, filter, jmxBuilder, entryPoint, retryPolicy)

  def withRetryPolicy(rp: RetryPolicy[F]): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, metricFilter, jmxBuilder, entryPoint, rp)

  def addMetricSet(ms: MetricSet): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, ms :: metricSet, metricFilter, jmxBuilder, entryPoint, retryPolicy)

  private val initStatus: F[ServiceParams] = for {
    uuid <- UUIDGen.randomUUID
    ts <- F.realTimeInstant
  } yield serviceConfig.evalConfig(uuid, ts, retryPolicy.show)

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
                    publisher.metricReport(
                      serviceParams,
                      metricRegistry,
                      metricFilter,
                      MetricReportType.Scheduled(t._2)))
              case Some(ScheduleType.Cron(cron)) =>
                cronScheduler
                  .awakeEvery(cron)
                  .zipWithIndex
                  .evalMap(t =>
                    publisher.metricReport(
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
                  .evalMap(_ => publisher.metricReset(serviceParams, metricRegistry, Some(cron)))
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

          val agent = new Agent[F](metricRegistry, channel, serviceParams, entryPoint)
          // put together
          channel.stream
            .onFinalize(channel.close.void) // drain pending send operation
            .mergeHaltL(metricsReset)
            .mergeHaltL(metricsReport)
            .concurrently(new ReStart[F, A](channel, serviceParams, retryPolicy, runAgent(agent)).stream)
            .concurrently(jmxReporting)
      }
    } yield event
}
