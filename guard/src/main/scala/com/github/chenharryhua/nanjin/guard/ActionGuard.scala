package com.github.chenharryhua.nanjin.guard

import cats.Functor
import cats.data.{Kleisli, Reader}
import cats.syntax.all._
import fs2.concurrent.Topic

final class ActionGuard[F[_]](
  topic: Topic[F, NJEvent],
  serviceInfo: ServiceInfo,
  actionName: String,
  config: ActionConfig) {

  def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](topic, serviceInfo, actionName, f(config))

  def retry[A, B](input: A)(f: A => F[B]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      topic,
      serviceInfo,
      actionName,
      config,
      input,
      Kleisli(f),
      Reader(_ => ""),
      Reader(_ => ""))

  def retry[B](f: F[B]): ActionRetry[F, Unit, B] = retry[Unit, B](())(_ => f)

  def fyi(msg: String)(implicit F: Functor[F]): F[Unit] =
    topic.publish1(ForYouInformation(serviceInfo.applicationName, msg)).void

  def retryEither[A, B](input: A)(f: A => F[Either[Throwable, B]]): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      topic,
      serviceInfo,
      actionName,
      config,
      input,
      Kleisli(f),
      Reader(_ => ""),
      Reader(_ => "")
    )

  def retryEither[B](f: F[Either[Throwable, B]]): ActionRetryEither[F, Unit, B] =
    retryEither[Unit, B](())(_ => f)
}
