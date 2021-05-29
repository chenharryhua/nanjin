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

  def eventStream[A](watch: (String => ActionGuard[F]) => F[A])(implicit F: Async[F]): Stream[F, NJEvent] = {
    val log = new LogService[F]()
    for {
      ts <- Stream.eval(F.realTimeInstant)
      serviceInfo: ServiceInfo =
        ServiceInfo(
          applicationName = applicationName,
          serviceName = serviceName,
          retryPolicy = params.retryPolicy.policy[F].show,
          launchTime = ts,
          healthCheck = params.healthCheck,
          isNormalStop = params.isNormalStop
        )

      ssd = ServiceStarted(serviceInfo)
      shc = ServiceHealthCheck(serviceInfo)
      sos = ServiceStopped(serviceInfo)
      event <- Stream
        .eval(Topic[F, NJEvent])
        .flatMap { topic =>
          val publisher = Stream.eval(
            retry.retryingOnAllErrors(
              params.retryPolicy.policy[F],
              (e: Throwable, r) => topic.publish1(ServicePanic(serviceInfo, r, e)).void) {
              (topic.publish1(ssd).delayBy(params.retryPolicy.value).void <*
                topic.publish1(shc).delayBy(params.healthCheck.interval).foreverM).background.use(_ =>
                watch(actionName => new ActionGuard[F](topic, serviceInfo, actionName, actionConfig))) >>
                topic.publish1(sos).guarantee(topic.close.void)
            })
          val consumer = topic.subscribe(params.topicMaxQueued)
          consumer.concurrently(publisher)
        }
        .evalTap(log.alert)
    } yield event
  }
}
