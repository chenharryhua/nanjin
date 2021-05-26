package com.github.chenharryhua.nanjin.guard

import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails

final class ServiceGuard[F[_]](
  applicationName: String,
  serviceName: String,
  alertServices: NonEmptyList[AlertService[F]],
  config: ServiceConfig) {
  val params: ServiceParams = config.evalConfig

  def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](applicationName, serviceName, alertServices, f(config))

  def run[A](serviceUp: F[A])(implicit F: Async[F]): F[Unit] = F.realTimeInstant.flatMap { ts =>
    val serviceInfo: ServiceInfo =
      ServiceInfo(serviceName, params.retryPolicy.policy[F].show, ts, params.healthCheck)

    val healthCheck: F[Unit] =
      alertServices
        .traverse(_.alert(ServiceHealthCheck(applicationName = applicationName, serviceInfo = serviceInfo)).attempt)
        .delayBy(params.healthCheck.interval)
        .void
        .foreverM

    val startUp: F[Unit] =
      alertServices
        .traverse(_.alert(ServiceStarted(applicationName = applicationName, serviceInfo = serviceInfo)).attempt)
        .delayBy(params.retryPolicy.value)
        .void

    val abnormalStop: F[Unit] =
      alertServices
        .traverse(_.alert(ServiceAbnormalStop(applicationName = applicationName, serviceInfo = serviceInfo)).attempt)
        .void

    def onError(error: Throwable, retryDetails: RetryDetails): F[Unit] =
      alertServices
        .traverse(
          _.alert(
            ServicePanic(
              applicationName = applicationName,
              serviceInfo = serviceInfo,
              retryDetails = retryDetails,
              error = error
            )).attempt)
        .void

    retry.retryingOnAllErrors(params.retryPolicy.policy[F], onError) {
      (startUp >> healthCheck).background.use(_ => serviceUp) >> abnormalStop
    }
  }

  def run[A](streamUp: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    Stream.eval(run(streamUp.compile.drain))

}
