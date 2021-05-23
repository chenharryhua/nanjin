package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}

import java.time.Instant

final class ServiceGuard[F[_]](alertServices: List[AlertService[F]], config: ServiceConfig) {
  val params: ServiceParams = config.evalConfig

  def updateConfig(f: ServiceConfig => ServiceConfig): ServiceGuard[F] =
    new ServiceGuard[F](alertServices, f(config))

  private def healthCheck(launchTime: Instant)(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(
        _.alert(
          ServiceHealthCheck(
            applicationName = params.applicationName,
            serviceName = params.serviceName,
            launchTime = launchTime,
            healthCheckInterval = params.healthCheckInterval)).attempt)
      .delayBy(params.healthCheckInterval)
      .void
      .foreverM[Unit]

  private def start(launchTime: Instant)(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(
        _.alert(
          ServiceStarted(
            applicationName = params.applicationName,
            serviceName = params.serviceName,
            launchTime = launchTime)).attempt)
      .void
      .delayBy(params.retryPolicy.value)

  private def abnormalStop(launchTime: Instant)(implicit F: Async[F]): F[Unit] =
    alertServices
      .traverse(
        _.alert(ServiceAbnormalStop(
          applicationName = params.applicationName,
          serviceName = params.serviceName,
          launchTime = launchTime,
          error = new Exception(s"service(${params.serviceName}) was abnormally stopped.")
        )).attempt)
      .delayBy(params.retryPolicy.value)
      .void
      .foreverM[Unit]

  def run[A](action: F[A])(implicit F: Async[F]): F[Unit] = {

    def onError(launchTime: Instant)(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(ServiceRestarting(
                applicationName = params.applicationName,
                serviceName = params.serviceName,
                launchTime = launchTime,
                willDelayAndRetry = wd,
                retryPolicy = params.retryPolicy.policy[F].show,
                error = err
              )).attempt)
            .void
        case GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(
                ServiceAbnormalStop(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  launchTime = launchTime,
                  error = err)).attempt)
            .void
      }
    F.realTimeInstant.flatMap(launchTime =>
      retry.retryingOnAllErrors(params.retryPolicy.policy[F], onError(launchTime))(
        F.bracket((start(launchTime) *> healthCheck(launchTime)).start)(_ => action)(_.cancel)
          .flatMap(_ => abnormalStop(launchTime))))
  }

  def run[A](stream: Stream[F, A])(implicit F: Async[F]): Stream[F, Unit] =
    Stream.eval(run(stream.compile.drain))

}
