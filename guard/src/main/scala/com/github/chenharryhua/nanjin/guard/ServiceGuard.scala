package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import fs2.concurrent.Topic

final class ServiceGuard[F[_]](
  applicationName: String,
  serviceName: String,
  serviceConfig: ServiceConfig,
  actionConfig: ActionConfig) {
  val params: ServiceParams = serviceConfig.evalConfig

  def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, f(serviceConfig), actionConfig)

  def run[A](serviceUp: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, Event] = {
    val log = new LogService[F]()
    for {
      ts <- Stream.eval(F.realTimeInstant)
      serviceInfo: ServiceInfo =
        ServiceInfo(applicationName, serviceName, params.retryPolicy.policy[F].show, ts, params.healthCheck)
      shc = ServiceHealthCheck(serviceInfo)
      ssd = ServiceStarted(serviceInfo)
      sos = ServiceAbnormalStop(serviceInfo)
      event <- Stream
        .eval(Topic[F, Event])
        .flatMap { topic =>
          val publisher = Stream.eval(
            retry.retryingOnAllErrors(
              params.retryPolicy.policy[F],
              (e: Throwable, r) => topic.publish1(ServicePanic(serviceInfo, r, e)).void) {
              (topic.publish1(ssd).delayBy(params.retryPolicy.value).void <*
                topic.publish1(shc).delayBy(params.healthCheck.interval).foreverM).background.use(_ =>
                serviceUp(actionName => new ActionGuard[F](topic, serviceInfo, actionName, actionConfig))) >>
                topic.publish1(sos)
            })
          val consumer = topic.subscribe(10)
          consumer.concurrently(publisher)
        }
        .evalTap(log.alert)
    } yield event
  }
}
