package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.action.ActionGuard
import com.github.chenharryhua.nanjin.guard.alert._
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import fs2.Stream
import fs2.concurrent.Channel

import java.util.UUID

/** Service never stop
  * @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name") val es: Stream[IO,NJEvent] = guard.eventStream{
  *   gd => gd("action-1").retry(IO(1)).run >> IO("other computation") >> gd("action-2").retry(IO(2)).run } }}}
  */

final class ServiceGuard[F[_]](
  serviceName: String,
  appName: String,
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig) {
  val params: ServiceParams = serviceConfig.evalConfig

  def updateServiceConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, appName, f(serviceConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): ServiceGuard[F] =
    new ServiceGuard[F](serviceName, appName, serviceConfig, f(actionConfig))

  def eventStream[A](actionGuard: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, NJEvent] =
    for {
      ts <- Stream.eval(F.realTimeInstant)
      serviceInfo: ServiceInfo =
        ServiceInfo(
          serviceName = serviceName,
          appName = appName,
          params = params,
          launchTime = ts
        )
      ssd = ServiceStarted(serviceInfo)
      shc = ServiceHealthCheck(serviceInfo)
      sos = ServiceStoppedAbnormally(serviceInfo)
      event <- Stream.eval(Channel.unbounded[F, NJEvent]).flatMap { channel =>
        val publisher = Stream.eval {
          val ret = retry.retryingOnAllErrors(
            params.retryPolicy.policy[F],
            (e: Throwable, r) => channel.send(ServicePanic(serviceInfo, r, UUID.randomUUID(), NJError(e))).void) {
            (channel.send(ssd).delayBy(params.startUpEventDelay).void <*
              channel.send(shc).delayBy(params.healthCheck.interval).foreverM).background.use(_ =>
              actionGuard(actionName =>
                new ActionGuard[F](
                  channel = channel,
                  actionName = actionName,
                  serviceName = serviceName,
                  appName = appName,
                  actionConfig = actionConfig))) *>
              channel.send(sos)
          }
          // should never return, but if it did, close the topic so that the whole stream will be stopped
          ret.guarantee(channel.close.void)
        }
        channel.stream.concurrently(publisher)
      }
    } yield event
}
