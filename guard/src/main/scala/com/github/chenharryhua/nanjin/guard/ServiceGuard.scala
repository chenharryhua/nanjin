package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Async
import cats.effect.std.Dispatcher
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.alert.*
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import cron4s.Cron
import cron4s.expr.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.Stream
import fs2.concurrent.Channel

import java.util.UUID

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

final class ServiceGuard[F[_]](serviceConfig: ServiceConfig) extends UpdateConfig[ServiceConfig, ServiceGuard[F]] {
  val params: ServiceParams = serviceConfig.evalConfig

  override def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig))

  def eventStream[A](actionGuard: ActionGuard[F] => F[A])(implicit F: Async[F]): Stream[F, NJEvent] = {
    val scheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))
    val cron: CronExpr                    = Cron.unsafeParse(s"0 0 ${params.taskParams.dailySummaryReset.hour} ? * *")
    val serviceInfo: F[ServiceInfo] =
      realZonedDateTime(params).map(ts => ServiceInfo(id = UUID.randomUUID(), launchTime = ts))

    for {
      si <- Stream.eval(serviceInfo)
      dailySummaries <- Stream.eval(F.ref(DailySummaries.zero))
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
                _ <- dailySummaries.update(_.incServicePanic)
              } yield ()
          ) {
            val start_health: F[Unit] = for { // fire service startup event and then health-check events
              ts <- realZonedDateTime(params)
              _ <- channel
                .send(ServiceStarted(timestamp = ts, serviceInfo = si, serviceParams = params))
                .delayBy(params.startUpEventDelay)
              _ <- dailySummaries.get.flatMap { ds =>
                for {
                  ts <- realZonedDateTime(params)
                  _ <- channel.send(ServiceHealthCheck(
                    timestamp = ts,
                    serviceInfo = si,
                    serviceParams = params,
                    dailySummaries = ds,
                    totalMemory = Runtime.getRuntime.totalMemory,
                    freeMemory = Runtime.getRuntime.freeMemory
                  ))
                } yield ()
              }.delayBy(params.healthCheck.interval).foreverM[Unit]
            } yield ()

            (start_health.background, Dispatcher[F]).tupled.use { case (_, dispatcher) =>
              actionGuard(
                new ActionGuard[F](
                  serviceInfo = si,
                  dispatcher = dispatcher,
                  dailySummaries = dailySummaries,
                  channel = channel,
                  actionName = "anonymous",
                  actionConfig = ActionConfig(params)))
            }
          }
          .guarantee(realZonedDateTime(params).flatMap(ts =>
            channel.send(ServiceStopped(timestamp = ts, serviceInfo = si, serviceParams = params))) *> // stop event
            channel.close.void) // close channel and the stream as well

        channel.stream
          .concurrently(Stream.eval(service))
          .concurrently(
            scheduler
              .awakeEvery(cron)
              .evalMap(_ =>
                for {
                  ts <- realZonedDateTime(params)
                  ds <- dailySummaries.getAndUpdate(_.reset)
                  _ <- channel.send(
                    ServiceDailySummariesReset(
                      timestamp = ts,
                      serviceInfo = si,
                      serviceParams = params,
                      dailySummaries = ds
                    ))
                } yield ()))
      }
    } yield event
  }
}
