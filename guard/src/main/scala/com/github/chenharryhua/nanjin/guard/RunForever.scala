package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import cats.effect.syntax.all._
import cats.syntax.all._
import fs2.Stream
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies, Sleep}

import scala.util.control.NonFatal

final private class RunForever[F[_]](alertServices: List[AlertService[F]], config: ServiceConfig) {
  private val params: ServiceParams = config.evalConfig

  def nonStop[A](action: F[A])(implicit F: Async[F], sleep: Sleep[F]): F[Unit] = {

    def onError(err: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wd @ WillDelayAndRetry(_, _, _) =>
          alertServices
            .traverse(
              _.alert(
                ServiceRestarting(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  willDelayAndRetry = wd,
                  error = err)))
            .void
        case GivingUp(_, _) =>
          alertServices
            .traverse(
              _.alert(
                ServiceAbnormalStop(
                  applicationName = params.applicationName,
                  serviceName = params.serviceName,
                  error = err)))
            .void
      }

    val healthChecking: F[Nothing] =
      alertServices
        .traverse(
          _.alert(
            ServiceHealthCheck(
              applicationName = params.applicationName,
              serviceName = params.serviceName,
              healthCheckInterval = params.healthCheckInterval)))
        .void
        .delayBy(params.healthCheckInterval.value)
        .foreverM

    val startService: F[Unit] =
      alertServices
        .traverse(_.alert(ServiceStarted(applicationName = params.applicationName, serviceName = params.serviceName)))
        .void
        .delayBy(params.restartInterval.value)

    val enrich: F[Unit] = for {
      fiber <- (startService <* healthChecking).start
      _ <- action.onCancel(fiber.cancel).onError { case _ => fiber.cancel }
      _ <- alertServices
        .traverse(
          _.alert(ServiceAbnormalStop(
            applicationName = params.applicationName,
            serviceName = params.serviceName,
            error = new Exception(s"service(${params.serviceName.value}) was abnormally stopped.")
          )))
        .void
    } yield ()

    retry.retryingOnSomeErrors(
      RetryPolicies.constantDelay[F](params.restartInterval.value),
      (e: Throwable) => F.delay(NonFatal(e)),
      onError)(enrich)
  }

  def infiniteStream[A](stream: Stream[F, A])(implicit F: Async[F], sleep: Sleep[F]): Stream[F, Unit] =
    Stream.eval(nonStop(stream.compile.drain))

}
