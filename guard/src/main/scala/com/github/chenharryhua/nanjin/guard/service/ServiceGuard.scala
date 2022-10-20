package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Console, UUIDGen}
import cats.syntax.all.*
import cats.Endo
import cats.effect.implicits.genSpawnOps
import com.codahale.metrics.{MetricFilter, MetricRegistry, MetricSet}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.config.{ScheduleType, ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.Translator
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
  restartPolicy: RetryPolicy[F])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig), metricSet, metricFilter, jmxBuilder, entryPoint, restartPolicy)

  def apply(serviceName: ServiceName): ServiceGuard[F] =
    updateConfig(_.withServiceName(serviceName))

  def withJmxReporter(builder: Endo[JmxReporter.Builder]): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, metricFilter, Some(builder), entryPoint, restartPolicy)

  def withMetricFilter(filter: MetricFilter): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, filter, jmxBuilder, entryPoint, restartPolicy)

  def withRestartPolicy(rp: RetryPolicy[F]): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, metricSet, metricFilter, jmxBuilder, entryPoint, rp)

  def addMetricSet(ms: MetricSet): ServiceGuard[F] =
    new ServiceGuard[F](serviceConfig, ms :: metricSet, metricFilter, jmxBuilder, entryPoint, restartPolicy)

  private val initStatus: F[ServiceParams] = for {
    uuid <- UUIDGen.randomUUID
    ts <- F.realTimeInstant
  } yield serviceConfig.evalConfig(uuid, ts, restartPolicy.show)

  def dummyAgent(implicit C: Console[F]): Resource[F, Agent[F]] = for {
    sp <- Resource.eval(initStatus)
    chn <- Resource.eval(Channel.bounded[F, NJEvent](sp.queueCapacity))
    _ <- chn.stream
      .evalMap(evt => Translator.simpleText[F].translate(evt).flatMap(_.traverse(C.println)))
      .compile
      .drain
      .background
  } yield new Agent[F](sp, new MetricRegistry, chn, entryPoint)

  def eventStream[A](runAgent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceParams <- Stream.eval(initStatus)
      event <- Stream.eval(Channel.bounded[F, NJEvent](serviceParams.queueCapacity)).flatMap { channel =>
        val metricRegistry: MetricRegistry = {
          val mr = new MetricRegistry()
          metricSet.foreach(mr.registerAll)
          mr
        }

        val cronScheduler: CronScheduler = new CronScheduler(serviceParams.taskParams.zoneId)

        val metricsReport: Stream[F, Nothing] =
          serviceParams.metricParams.reportSchedule match {
            case Some(ScheduleType.Fixed(dur)) =>
              // https://stackoverflow.com/questions/24649842/scheduleatfixedrate-vs-schedulewithfixeddelay
              Stream
                .fixedRate[F](dur.toScala)
                .zipWithIndex
                .evalTap(t =>
                  publisher.metricReport(
                    channel,
                    serviceParams,
                    metricRegistry,
                    metricFilter,
                    MetricReportType.Scheduled(t._2)))
                .drain
            case Some(ScheduleType.Cron(cron)) =>
              cronScheduler
                .awakeEvery(cron)
                .evalMap(idx =>
                  publisher.metricReport(
                    channel,
                    serviceParams,
                    metricRegistry,
                    metricFilter,
                    MetricReportType.Scheduled(idx)))
                .drain
            case None => Stream.empty
          }

        val metricsReset: Stream[F, Nothing] =
          serviceParams.metricParams.resetSchedule match {
            case None => Stream.empty
            case Some(cron) =>
              cronScheduler
                .awakeEvery(cron)
                .evalMap(_ => publisher.metricReset(channel, serviceParams, metricRegistry, Some(cron)))
                .drain
          }

        val jmxReporting: Stream[F, Nothing] =
          jmxBuilder match {
            case None => Stream.empty
            case Some(builder) =>
              Stream
                .bracket(
                  F.delay(builder(JmxReporter.forRegistry(metricRegistry)).filter(metricFilter).build()))(r =>
                  F.delay(r.close()))
                .evalMap(jr => F.delay(jr.start()))
                .flatMap(_ => Stream.never[F])
          }

        val agent = new Agent[F](serviceParams, metricRegistry, channel, entryPoint)
        // put together
        channel.stream
          .concurrently(metricsReset)
          .concurrently(metricsReport)
          .concurrently(jmxReporting)
          .concurrently(new ReStart[F, A](channel, serviceParams, restartPolicy, runAgent(agent)).stream)
      }
    } yield event
}
