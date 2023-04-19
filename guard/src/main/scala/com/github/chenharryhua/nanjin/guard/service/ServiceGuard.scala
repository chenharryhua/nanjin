package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.{Async, Resource, Unique}
import cats.effect.std.{AtomicCell, Console, Dispatcher, UUIDGen}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.jmx.JmxReporter
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.ServiceName
import com.github.chenharryhua.nanjin.guard.config.{Measurement, ServiceConfig, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.translators.Translator
import com.github.chenharryhua.nanjin.guard.policies
import cron4s.CronExpr
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import fs2.io.net.Network
import io.circe.Json
import natchez.EntryPoint
import org.http4s.ember.server.EmberServerBuilder
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

final class ServiceGuard[F[_]: Network] private[guard] (
  serviceName: ServiceName,
  serviceConfig: ServiceConfig,
  entryPoint: Resource[F, EntryPoint[F]],
  restartPolicy: RetryPolicy[F],
  jmxBuilder: Option[Endo[JmxReporter.Builder]],
  httpBuilder: Option[Endo[EmberServerBuilder[F]]],
  brief: F[Json])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] { self =>

  private def copy(
    serviceName: ServiceName = self.serviceName,
    serviceConfig: ServiceConfig = self.serviceConfig,
    restartPolicy: RetryPolicy[F] = self.restartPolicy,
    jmxBuilder: Option[JmxReporter.Builder => JmxReporter.Builder] = self.jmxBuilder,
    httpBuilder: Option[EmberServerBuilder[F] => EmberServerBuilder[F]] = self.httpBuilder,
    brief: F[Json] = self.brief
  ): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, serviceConfig, entryPoint, restartPolicy, jmxBuilder, httpBuilder, brief)

  override def updateConfig(f: Endo[ServiceConfig]): ServiceGuard[F] = copy(serviceConfig = f(serviceConfig))
  def apply(serviceName: ServiceName): ServiceGuard[F]               = copy(serviceName = serviceName)

  /** https://cb372.github.io/cats-retry/docs/policies.html
    */
  def withRestartPolicy(rp: RetryPolicy[F]): ServiceGuard[F] = copy(restartPolicy = rp)

  def withRestartPolicy(cronExpr: CronExpr): ServiceGuard[F] =
    withRestartPolicy(policies.cronBackoff[F](cronExpr, serviceConfig.taskParams.zoneId))

  def withJmx(f: Endo[JmxReporter.Builder]): ServiceGuard[F] =
    copy(jmxBuilder = Some(f))

  def withMetricServer(f: Endo[EmberServerBuilder[F]]): ServiceGuard[F] =
    copy(httpBuilder = Some(f))

  def withBrief(json: F[Json]): ServiceGuard[F] = copy(brief = json)
  def withBrief(json: Json): ServiceGuard[F]    = copy(brief = F.pure(json))

  private lazy val initStatus: F[ServiceParams] = for {
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
    dispatcher = dispatcher,
    measurement = Measurement(sp.serviceName.value)
  )

  def eventStream[A](runAgent: GeneralAgent[F] => F[A]): Stream[F, NJEvent] =
    for {
      serviceParams <- Stream.eval(initStatus)
      signallingMapRef <- Stream.eval(SignallingMapRef.ofSingleImmutableMap[F, Unique.Token, Locker]())
      atomicCell <- Stream.eval(AtomicCell[F].of(Vault.empty))
      dispatcher <- Stream.resource(Dispatcher.parallel[F])
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val metricRegistry: MetricRegistry = new MetricRegistry()

        val metricsReport: Stream[F, Nothing] =
          serviceParams.metricParams.reportSchedule match {
            case None => Stream.empty
            case Some(cron) =>
              awakeEvery(policies.cronBackoff[F](cron, serviceParams.taskParams.zoneId))
                .evalMap(tick =>
                  publisher.metricReport(
                    channel = channel,
                    serviceParams = serviceParams,
                    metricRegistry = metricRegistry,
                    index = MetricIndex.Periodic(tick.index)))
                .drain
          }

        val metricsReset: Stream[F, Nothing] =
          serviceParams.metricParams.resetSchedule match {
            case None => Stream.empty
            case Some(cron) =>
              awakeEvery(policies.cronBackoff[F](cron, serviceParams.taskParams.zoneId))
                .evalMap(tick =>
                  publisher.metricReset(
                    channel = channel,
                    serviceParams = serviceParams,
                    metricRegistry = metricRegistry,
                    index = MetricIndex.Periodic(tick.index)))
                .drain
          }

        val jmxReporting: Stream[F, Nothing] =
          jmxBuilder match {
            case None => Stream.empty
            case Some(build) =>
              Stream.bracket(F.blocking {
                val reporter =
                  build(
                    JmxReporter
                      .forRegistry(metricRegistry)
                      .convertDurationsTo(serviceParams.metricParams.durationTimeUnit)
                      .convertRatesTo(serviceParams.metricParams.rateTimeUnit))
                    .createsObjectNamesWith(objectNameFactory) // respect builder except object name factory
                    .build()
                reporter.start()
                reporter
              })(r => F.blocking(r.stop())) >> Stream.never[F]
          }

        val metricsServer: Stream[F, Nothing] =
          httpBuilder match {
            case None => Stream.empty
            case Some(build) =>
              Stream.resource(
                build(EmberServerBuilder.default[F].withHost(ip"0.0.0.0").withPort(port"1026"))
                  .withHttpApp(new MetricsRouter[F](metricRegistry, serviceParams).router)
                  .build) >> Stream.never[F]
          }

        val agent: GeneralAgent[F] =
          new GeneralAgent[F](
            serviceParams = serviceParams,
            metricRegistry = metricRegistry,
            channel = channel,
            entryPoint = entryPoint,
            signallingMapRef = signallingMapRef,
            atomicCell = atomicCell,
            dispatcher = dispatcher,
            measurement = Measurement(serviceParams.serviceName.value)
          )

        val surveillance: Stream[F, Nothing] =
          new ReStart[F, A](channel, serviceParams, restartPolicy, runAgent(agent)).stream

        // put together
        channel.stream
          .concurrently(jmxReporting)
          .concurrently(metricsReset)
          .concurrently(metricsReport)
          .concurrently(metricsServer)
          .concurrently(surveillance)
      }
    } yield event
}
