package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Resource, Unique}
import cats.effect.std.{Console, Dispatcher, UUIDGen}
import cats.syntax.all.*
import cats.Endo
import cats.effect.implicits.genSpawnOps
import com.codahale.metrics.{MetricFilter, MetricRegistry, MetricSet}
import com.codahale.metrics.jmx.JmxReporter
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.{awakeEvery, policies}
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, ServiceParams, TaskParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.Translator
import cron4s.CronExpr
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import io.circe.Json
import natchez.EntryPoint
import org.typelevel.vault.Locker
import retry.RetryPolicy

// format: off
/** @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name") 
  *       val es: Stream[IO,NJEvent] = guard.eventStream {
  *           gd => gd.action("action-1").retry(IO(1)).run >>
  *                  IO("other computation") >> 
  *                  gd.action("action-2").retry(IO(2)).run
  *            }
  * }}}
  */
// format: on

final class ServiceGuard[F[_]] private[guard] (
  serviceName: ServiceName,
  taskParams: TaskParams,
  serviceConfig: ServiceConfig,
  metricSet: List[MetricSet],
  jmxBuilder: Option[Endo[JmxReporter.Builder]],
  entryPoint: Resource[F, EntryPoint[F]],
  restartPolicy: RetryPolicy[F],
  brief: F[Json])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  private def copy(
    serviceName: ServiceName = serviceName,
    serviceConfig: ServiceConfig = serviceConfig,
    metricSet: List[MetricSet] = metricSet,
    jmxBuilder: Option[Endo[JmxReporter.Builder]] = jmxBuilder,
    restartPolicy: RetryPolicy[F] = restartPolicy,
    brief: F[Json] = brief
  ): ServiceGuard[F] = new ServiceGuard[F](
    serviceName,
    taskParams,
    serviceConfig,
    metricSet,
    jmxBuilder,
    entryPoint,
    restartPolicy,
    brief)

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] = copy(serviceConfig = f(serviceConfig))
  def apply(serviceName: ServiceName): ServiceGuard[F]               = copy(serviceName = serviceName)

  /** https://metrics.dropwizard.io/4.2.0/getting-started.html#reporting-via-jmx
    */
  def withJmxReporter(builder: Endo[JmxReporter.Builder]): ServiceGuard[F] = copy(jmxBuilder = Some(builder))

  /** https://cb372.github.io/cats-retry/docs/policies.html
    */
  def withRestartPolicy(rp: RetryPolicy[F]): ServiceGuard[F] = copy(restartPolicy = rp)

  def withRestartPolicy(cronExpr: CronExpr): ServiceGuard[F] =
    withRestartPolicy(policies.cronBackoff[F](cronExpr, taskParams.zoneId))

  def withBrief(json: F[Json]): ServiceGuard[F] = copy(brief = json)
  def withBrief(json: Json): ServiceGuard[F]    = copy(brief = F.pure(json))

  /** https://metrics.dropwizard.io/4.2.0/manual/core.html#metric-sets
    */
  def addMetricSet(ms: MetricSet): ServiceGuard[F] = copy(metricSet = ms :: metricSet)

  private val initStatus: F[ServiceParams] = for {
    uuid <- UUIDGen.randomUUID
    ts <- F.realTimeInstant
    json <- brief
  } yield serviceConfig.evalConfig(serviceName, taskParams, uuid, ts, restartPolicy.show, json)

  def dummyAgent(implicit C: Console[F]): Resource[F, Agent[F]] = for {
    sp <- Resource.eval(initStatus)
    vault <- Resource.eval(SignallingMapRef.ofSingleImmutableMap[F, Unique.Token, Locker]())
    dispatcher <- Dispatcher.parallel[F]
    chn <- Resource.eval(Channel.unbounded[F, NJEvent])
    _ <- chn.stream
      .evalMap(evt => Translator.simpleText[F].translate(evt).flatMap(_.traverse(C.println)))
      .compile
      .drain
      .background
  } yield new Agent[F](sp, new MetricRegistry, chn, entryPoint, vault, dispatcher)

  def eventStream[A](runAgent: Agent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceParams <- Stream.eval(initStatus)
      vault <- Stream.eval(SignallingMapRef.ofSingleImmutableMap[F, Unique.Token, Locker]())
      dispatcher <- Stream.resource(Dispatcher.parallel[F])
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val metricRegistry: MetricRegistry = {
          val mr = new MetricRegistry()
          metricSet.foreach(mr.registerAll)
          mr
        }

        val metricsReport: Stream[F, Nothing] =
          serviceParams.metricParams.reportSchedule match {
            case None => Stream.empty
            case Some(cron) =>
              awakeEvery[F](policies.cronBackoff(cron, serviceParams.taskParams.zoneId))
                .evalMap(idx =>
                  publisher.metricReport(
                    channel,
                    serviceParams,
                    metricRegistry,
                    MetricFilter.ALL,
                    MetricIndex.Periodic(idx)))
                .drain
          }

        val metricsReset: Stream[F, Nothing] =
          serviceParams.metricParams.resetSchedule match {
            case None => Stream.empty
            case Some(cron) =>
              awakeEvery[F](policies.cronBackoff(cron, serviceParams.taskParams.zoneId))
                .evalMap(idx =>
                  publisher.metricReset(channel, serviceParams, metricRegistry, MetricIndex.Periodic(idx)))
                .drain
          }

        val jmxReporting: Stream[F, Nothing] =
          jmxBuilder match {
            case None => Stream.empty
            case Some(builder) =>
              Stream
                .bracket(F.blocking(builder(JmxReporter.forRegistry(metricRegistry)).build()))(r =>
                  F.blocking(r.close()))
                .evalMap(jr => F.blocking(jr.start()))
                .flatMap(_ => Stream.never[F])
          }

        val agent = new Agent[F](serviceParams, metricRegistry, channel, entryPoint, vault, dispatcher)

        // put together
        channel.stream
          .concurrently(metricsReset)
          .concurrently(metricsReport)
          .concurrently(jmxReporting)
          .concurrently(new ReStart[F, A](channel, serviceParams, restartPolicy, runAgent(agent)).stream)
      }
    } yield event
}
