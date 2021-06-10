package com.github.chenharryhua.nanjin.guard

import cats.effect.syntax.all._
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.action.ActionGuard
import com.github.chenharryhua.nanjin.guard.alert._
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import cron4s.Cron
import cron4s.expr.CronExpr
import eu.timepit.fs2cron.Scheduler
import eu.timepit.fs2cron.cron4s.Cron4sScheduler
import fs2.Stream
import fs2.concurrent.Channel

import java.net.InetAddress
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

final class ServiceGuard[F[_]](serviceName: String, serviceConfig: ServiceConfig, actionConfig: ActionConfig) {
  val params: ServiceParams = serviceConfig.evalConfig

  def updateServiceConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, f(serviceConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, serviceConfig, f(actionConfig))

  def eventStream[A](actionGuard: ActionGuard[F] => F[A])(implicit F: Async[F]): Stream[F, NJEvent] = {
    val scheduler: Scheduler[F, CronExpr] = Cron4sScheduler.from(F.pure(params.zoneId))
    val cron: CronExpr                    = Cron.unsafeParse(s"0 0 ${params.dailySummaryReset} ? * *")
    for {
      ts <- Stream.eval(F.realTimeInstant.map(_.atZone(params.zoneId)))
      serviceInfo = ServiceInfo(
        serviceName = serviceName,
        hostName = InetAddress.getLocalHost.getHostName,
        params = params,
        id = UUID.randomUUID(),
        launchTime = ts)
      dailySummaries <- Stream.eval(Ref.of(DailySummaries.zero))
      ssd = ServiceStarted(ts, serviceInfo)
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val publisher = Stream.eval {
          val ret = retry.retryingOnAllErrors(
            params.retryPolicy.policy[F],
            (ex: Throwable, rd) =>
              for {
                ts <- F.realTimeInstant.map(_.atZone(params.zoneId))
                _ <- channel.send(ServicePanic(ts, serviceInfo, rd, UUID.randomUUID(), NJError(ex)))
                _ <- dailySummaries.update(_.incServicePanic)
              } yield ()
          ) {
            val start_health = for {
              _ <- channel.send(ssd).delayBy(params.startUpEventDelay)
              _ <- dailySummaries.get.flatMap { ds =>
                F.realTimeInstant.map(_.atZone(params.zoneId)).flatMap { ts =>
                  channel.send(ServiceHealthCheck(ts, serviceInfo, ds))
                }
              }.delayBy(params.healthCheck.interval).foreverM[Unit]
            } yield ()
            start_health.background.use(_ =>
              actionGuard(
                new ActionGuard[F](
                  dailySummaries = dailySummaries,
                  channel = channel,
                  actionName = "anonymous",
                  serviceInfo = serviceInfo,
                  actionConfig = actionConfig))) *>
              F.realTimeInstant
                .map(_.atZone(params.zoneId))
                .flatMap(ts => channel.send(ServiceStopped(ts, serviceInfo)))
          }
          ret.guarantee(channel.close.void)
        }
        channel.stream
          .concurrently(publisher)
          .concurrently(scheduler.awakeEvery(cron).evalMap(_ => dailySummaries.update(_.reset)))
      }
    } yield event
  }
}
