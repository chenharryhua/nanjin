package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.{Async, Resource, Unique}
import cats.effect.std.{AtomicCell, Console, Dispatcher, UUIDGen}
import cats.syntax.all.*
import com.codahale.metrics.{MetricFilter, MetricRegistry, MetricSet}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.config.{ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.Translator
import com.github.chenharryhua.nanjin.guard.{awakeEvery, policies}
import cron4s.CronExpr
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import io.circe.Json
import natchez.EntryPoint
import org.typelevel.vault.{Locker, Vault}
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
  serviceConfig: ServiceConfig,
  metricSet: List[MetricSet],
  entryPoint: Resource[F, EntryPoint[F]],
  restartPolicy: RetryPolicy[F],
  brief: F[Json])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {

  private def copy(
    serviceName: ServiceName = serviceName,
    serviceConfig: ServiceConfig = serviceConfig,
    metricSet: List[MetricSet] = metricSet,
    restartPolicy: RetryPolicy[F] = restartPolicy,
    brief: F[Json] = brief
  ): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, serviceConfig, metricSet, entryPoint, restartPolicy, brief)

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] = copy(serviceConfig = f(serviceConfig))
  def apply(serviceName: ServiceName): ServiceGuard[F]               = copy(serviceName = serviceName)

  /** https://cb372.github.io/cats-retry/docs/policies.html
    */
  def withRestartPolicy(rp: RetryPolicy[F]): ServiceGuard[F] = copy(restartPolicy = rp)

  def withRestartPolicy(cronExpr: CronExpr): ServiceGuard[F] =
    withRestartPolicy(policies.cronBackoff[F](cronExpr, serviceConfig.taskParams.zoneId))

  def withBrief(json: F[Json]): ServiceGuard[F] = copy(brief = json)
  def withBrief(json: Json): ServiceGuard[F]    = copy(brief = F.pure(json))

  /** https://metrics.dropwizard.io/4.2.0/manual/core.html#metric-sets
    */
  def addMetricSet(ms: MetricSet): ServiceGuard[F] = copy(metricSet = ms :: metricSet)

  private val initStatus: F[ServiceParams] = for {
    uuid <- UUIDGen.randomUUID
    ts <- F.realTimeInstant
    json <- brief
  } yield serviceConfig.evalConfig(serviceName, uuid, ts, restartPolicy.show, json)

  def dummyAgent(implicit C: Console[F]): Resource[F, GeneralAgent[F]] = for {
    sp <- Resource.eval(initStatus)
    signallingMapRef <- Resource.eval(SignallingMapRef.ofSingleImmutableMap[F, Unique.Token, Locker]())
    atomicCell <- Resource.eval(AtomicCell[F].of(Vault.empty))
    dispatcher <- Dispatcher.parallel[F]
    chn <- Resource.eval(Channel.unbounded[F, NJEvent])
    _ <- chn.stream
      .evalMap(evt => Translator.simpleText[F].translate(evt).flatMap(_.traverse(C.println)))
      .compile
      .drain
      .background
  } yield new GeneralAgent[F](
    serviceParams = sp,
    metricRegistry = new MetricRegistry,
    channel = chn,
    entryPoint = entryPoint,
    signallingMapRef = signallingMapRef,
    atomicCell = atomicCell,
    dispatcher = dispatcher
  )

  def eventStream[A](runAgent: GeneralAgent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceParams <- Stream.eval(initStatus)
      signallingMapRef <- Stream.eval(SignallingMapRef.ofSingleImmutableMap[F, Unique.Token, Locker]())
      atomicCell <- Stream.eval(AtomicCell[F].of(Vault.empty))
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

        val agent: GeneralAgent[F] =
          new GeneralAgent[F](
            serviceParams = serviceParams,
            metricRegistry = metricRegistry,
            channel = channel,
            entryPoint = entryPoint,
            signallingMapRef = signallingMapRef,
            atomicCell = atomicCell,
            dispatcher = dispatcher
          )

        // put together
        channel.stream
          .concurrently(metricsReset)
          .concurrently(metricsReport)
          .concurrently(new ReStart[F, A](channel, serviceParams, restartPolicy, runAgent(agent)).stream)
      }
    } yield event
}
