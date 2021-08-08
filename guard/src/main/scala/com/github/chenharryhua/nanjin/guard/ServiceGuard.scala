package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Dispatcher, UUIDGen}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.codahale.metrics.{MetricRegistry, MetricSet}
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.metrics.NJMetricReporter
import com.github.chenharryhua.nanjin.guard.alert.*
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import cron4s.Cron
import cron4s.expr.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.concurrent.Channel
import fs2.{INothing, Pipe, Stream}

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
  metricRegistry: MetricRegistry,
  serviceConfig: ServiceConfig,
  alertServices: Resource[F, AlertService[F]],
  reporters: List[NJMetricReporter])(implicit F: Async[F])
    extends UpdateConfig[ServiceConfig, ServiceGuard[F]] with AddAlertService[F, ServiceGuard[F]]
    with AddMetricReporter[ServiceGuard[F]] {

  val params: ServiceParams = serviceConfig.evalConfig

  def registerMetricSet(metrics: MetricSet): ServiceGuard[F] = {
    metricRegistry.registerAll(metrics)
    new ServiceGuard[F](metricRegistry, serviceConfig, alertServices, reporters)
  }

  override def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](metricRegistry, f(serviceConfig), alertServices, reporters)

  override def addAlertService(ras: Resource[F, AlertService[F]]): ServiceGuard[F] =
    new ServiceGuard[F](metricRegistry, serviceConfig, alertServices.flatMap(ass => ras.map(_ |+| ass)), reporters)

  override def addMetricReporter(reporter: NJMetricReporter): ServiceGuard[F] =
    new ServiceGuard[F](metricRegistry, serviceConfig, alertServices, reporter :: reporters)

  def eventStream[A](actionGuard: ActionGuard[F] => F[A]): Stream[F, NJEvent] = {
    val scheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))
    val cron: CronExpr                    = Cron.unsafeParse(s"0 0 ${params.taskParams.dailySummaryReset.hour} ? * *")
    val serviceInfo: F[ServiceInfo] = for {
      ts <- realZonedDateTime(params)
      uuid <- UUIDGen.randomUUID
    } yield ServiceInfo(id = uuid, launchTime = ts)

    for {
      si <- Stream.eval(serviceInfo)
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val service: F[A] = retry.mtl
          .retryingOnAllErrors(
            params.retry.policy[F],
            (ex: Throwable, rd) =>
              for {
                ts <- realZonedDateTime(params)
                _ <- channel.send(
                  ServicePanic(
                    timestamp = ts,
                    serviceInfo = si,
                    serviceParams = params,
                    retryDetails = rd,
                    error = NJError(ex)))
              } yield ()
          ) {
            val healthReport: F[Unit] = for {
              ts <- realZonedDateTime(params)
              _ <- channel.send(
                ServiceHealthCheck(
                  timestamp = ts,
                  serviceInfo = si,
                  serviceParams = params,
                  dailySummaries = DailySummaries(metricRegistry)
                ))
            } yield ()

            val start_health: F[Unit] = for { // fire service startup event and then health-check events
              ts <- realZonedDateTime(params)
              _ <- channel
                .send(ServiceStarted(timestamp = ts, serviceInfo = si, serviceParams = params))
                .delayBy(params.startUpEventDelay)
              _ <- healthReport.delayBy(params.healthCheck.interval).foreverM[Unit]
            } yield ()

            (start_health.background, Dispatcher[F]).tupled.use { case (_, dispatcher) =>
              actionGuard(
                new ActionGuard[F](
                  metricRegistry = metricRegistry,
                  serviceInfo = si,
                  dispatcher = dispatcher,
                  channel = channel,
                  actionName = "anonymous",
                  actionConfig = ActionConfig(params)))
            }
          }
          .guarantee(realZonedDateTime(params).flatMap(ts =>
            channel.send(ServiceStopped(timestamp = ts, serviceInfo = si, serviceParams = params))) *> // stop event
            channel.close.void) // close channel and the stream as well

        // notify alert services
        val notify: Pipe[F, NJEvent, INothing] = { events =>
          val alerts: Resource[F, AlertService[F]] = for {
            mr <- Resource.pure(new NJMetricRegistry[F](metricRegistry))
            as <- alertServices
            _ <-
              F.parTraverseN[List, NJMetricReporter, Nothing](Math.max(1, reporters.size))(reporters)(
                _.start[F](metricRegistry))
                .background
          } yield as |+| mr

          Stream.resource(alerts).flatMap(as => events.evalMap(as.alert)).drain
        }

        channel.stream
          .observe(notify)
          .concurrently(Stream.eval(service))
          .concurrently(
            scheduler
              .awakeEvery(cron)
              .evalMap(_ =>
                for {
                  ts <- realZonedDateTime(params)
                  _ <- channel.send(
                    ServiceDailySummariesReset(
                      timestamp = ts,
                      serviceInfo = si,
                      serviceParams = params,
                      dailySummaries = DailySummaries(metricRegistry)
                    ))
                } yield ()))
      }
    } yield event
  }
}
