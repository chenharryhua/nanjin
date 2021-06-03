package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.action.ActionGuard
import com.github.chenharryhua.nanjin.guard.alert._
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ServiceConfig, ServiceParams}
import fs2.Stream
import fs2.concurrent.Topic
import fs2.concurrent.Topic.Closed

import java.util.UUID
import scala.concurrent.duration._

/** Service never stop
  * @example
  *   {{{ val guard = TaskGuard[IO]("appName").service("service-name") val es: Stream[IO,NJEvent] = guard.eventStream{
  *   gd => gd("action-1").retry(IO(1)).run >> IO("other computation") >> gd("action-2").retry(IO(2)).run } }}}
  */

final class ServiceGuard[F[_]](
  applicationName: String,
  serviceName: String,
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig) {
  val params: ServiceParams = serviceConfig.evalConfig

  def updateServiceConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, f(serviceConfig), actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, serviceConfig, f(actionConfig))

  def eventStream[A](actionGuard: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, NJEvent] =
    for {
      ts <- Stream.eval(F.realTimeInstant)
      serviceInfo: ServiceInfo =
        ServiceInfo(
          applicationName = applicationName,
          serviceName = serviceName,
          params = params,
          launchTime = ts
        )
      ssd = ServiceStarted(serviceInfo)
      shc = ServiceHealthCheck(serviceInfo)
      sos = ServiceStoppedAbnormally(serviceInfo)
      event <- Stream.eval(Topic[F, NJEvent]).flatMap { topic =>
        val subscriber = topic.subscribe(params.topicMaxQueued)
        val publisher: Stream[F, Either[Closed, Unit]] = Stream.eval {
          val ret = retry.retryingOnAllErrors(
            params.retryPolicy.policy[F],
            (e: Throwable, r) => topic.publish1(ServicePanic(serviceInfo, r, UUID.randomUUID(), e)).void) {
            (topic.publish1(ssd).delayBy(params.startUpEventDelay).void <*
              topic.publish1(shc).delayBy(params.healthCheck.interval).foreverM).background.use(_ =>
              actionGuard(actionName =>
                new ActionGuard[F](topic, applicationName, serviceName, actionName, actionConfig))) *>
              topic.publish1(sos)
          }
          // should never return, but if it did, close the topic so that the whole stream will be stopped
          ret.guarantee(topic.close.void)
        }
        subscriber.concurrently(publisher.delayBy(10.milliseconds))
      }
    } yield event
}
