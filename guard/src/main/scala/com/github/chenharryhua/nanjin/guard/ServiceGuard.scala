package com.github.chenharryhua.nanjin.guard

import cats.effect.syntax.all._
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert._
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import cron4s.Cron
import cron4s.expr.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.Stream
import fs2.concurrent.Channel

import java.net.InetAddress
import java.time.ZonedDateTime
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

final class ServiceGuard[F[_]](serviceConfig: ServiceConfig) {
  val params: ServiceParams = serviceConfig.evalConfig

  def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](f(serviceConfig))

  def eventStream[A](actionGuard: ActionGuard[F] => F[A])(implicit F: Async[F]): Stream[F, NJEvent] = {
    val scheduler: Scheduler[F, CronExpr]   = Cron4sScheduler.from(F.pure(params.taskParams.zoneId))
    val cron: CronExpr                      = Cron.unsafeParse(s"0 0 ${params.taskParams.dailySummaryReset} ? * *")
    val realZonedDateTime: F[ZonedDateTime] = F.realTimeInstant.map(_.atZone(params.taskParams.zoneId))
    val serviceInfo: F[ServiceInfo] = realZonedDateTime.map(ts =>
      ServiceInfo(hostName = InetAddress.getLocalHost.getHostName, id = UUID.randomUUID(), launchTime = ts))

    for {
      si <- Stream.eval(serviceInfo)
      dailySummaries <- Stream.eval(Ref.of(DailySummaries.zero))
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val service: F[A] = retry
          .retryingOnAllErrors(
            params.retryPolicy.policy[F],
            (ex: Throwable, rd) =>
              for {
                ts <- realZonedDateTime
                _ <- channel.send(
                  ServicePanic(
                    timestamp = ts,
                    serviceInfo = si,
                    params = params,
                    retryDetails = rd,
                    errorID = UUID.randomUUID(),
                    error = NJError(ex)))
                _ <- dailySummaries.update(_.incServicePanic)
              } yield ()
          ) {
            val start_health: F[Unit] = for { // fire service startup event and then health-check events
              ts <- realZonedDateTime
              _ <- channel
                .send(ServiceStarted(timestamp = ts, serviceInfo = si, params = params))
                .delayBy(params.startUpEventDelay)
              _ <- dailySummaries.get.flatMap { ds =>
                realZonedDateTime.flatMap { ts =>
                  channel.send(
                    ServiceHealthCheck(
                      timestamp = ts,
                      serviceInfo = si,
                      params = params,
                      dailySummaries = ds,
                      totalMemory = Runtime.getRuntime.totalMemory,
                      freeMemory = Runtime.getRuntime.freeMemory
                    ))
                }
              }.delayBy(params.healthCheck.interval).foreverM[Unit]
            } yield ()

            start_health.background.use(_ =>
              actionGuard(
                new ActionGuard[F](
                  serviceInfo = si,
                  dailySummaries = dailySummaries,
                  channel = channel,
                  actionName = "anonymous",
                  actionConfig = ActionConfig(params))))
          }
          .guarantee(realZonedDateTime.flatMap(ts =>
            channel.send(ServiceStopped(timestamp = ts, serviceInfo = si, params = params))) *> channel.close.void)

        channel.stream
          .concurrently(Stream.eval(service))
          .concurrently(scheduler.awakeEvery(cron).evalMap(_ => dailySummaries.update(_.reset)))
      }
    } yield event
  }
}
