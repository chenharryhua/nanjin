package com.github.chenharryhua.nanjin.guard

import cats.Functor
import cats.data.{EitherT, Kleisli, Reader}
import cats.syntax.all._
import fs2.concurrent.Topic

final class ActionGuard[F[_]](
  topic: Topic[F, NJEvent],
  applicationName: String,
  parentName: String,
  actionName: String,
  config: ActionConfig) {

  def updateActionConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](topic, applicationName, parentName, actionName, f(config))

  def retry[A, B](input: A)(f: A => F[B]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      topic,
      applicationName,
      parentName,
      actionName,
      config,
      input,
      Kleisli(f),
      Reader(tuple2 => ""),
      Reader(tuple2 => ""))

  def retry[B](f: F[B]): ActionRetry[F, Unit, B] = retry[Unit, B](())(_ => f)

  def fyi(msg: String)(implicit F: Functor[F]): F[Unit] =
    topic.publish1(ForYouInformation(applicationName, msg)).void

  def retryEither[A, B](input: A)(f: A => F[Either[Throwable, B]]): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      topic,
      applicationName,
      parentName,
      actionName,
      config,
      input,
      EitherT(Kleisli(f)),
      Reader(tuple2 => ""),
      Reader(tuple2 => ""))

  def retryEither[B](f: F[Either[Throwable, B]]): ActionRetryEither[F, Unit, B] =
    retryEither[Unit, B](())(_ => f)
}
