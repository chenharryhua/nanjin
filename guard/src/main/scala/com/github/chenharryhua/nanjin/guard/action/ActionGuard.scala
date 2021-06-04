package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import cats.data.{EitherT, Kleisli, Reader}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{ForYouInformation, NJEvent}
import com.github.chenharryhua.nanjin.guard.config.ActionConfig
import fs2.concurrent.Channel

final class ActionGuard[F[_]](
  channel: Channel[F, NJEvent],
  actionName: String,
  serviceName: String,
  appName: String,
  actionConfig: ActionConfig) {

  def apply(actionName: String): ActionGuard[F] =
    new ActionGuard[F](channel, actionName, serviceName, appName, actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](channel, actionName, serviceName, appName, f(actionConfig))

  def retry[A, B](input: A)(f: A => F[B]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      channel = channel,
      actionName = actionName,
      serviceName = serviceName,
      appName = appName,
      actionConfig = actionConfig,
      input = input,
      kleisli = Kleisli(f),
      succ = Reader(_ => ""),
      fail = Reader(_ => ""))

  def retry[B](f: F[B]): ActionRetry[F, Unit, B] = retry[Unit, B](())(_ => f)

  def fyi(msg: String)(implicit F: Functor[F]): F[Unit] = channel.send(ForYouInformation(msg)).void

  def retryEither[A, B](input: A)(f: A => F[Either[Throwable, B]]): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      channel = channel,
      actionName = actionName,
      serviceName = serviceName,
      appName = appName,
      actionConfig = actionConfig,
      input = input,
      eitherT = EitherT(Kleisli(f)),
      succ = Reader(_ => ""),
      fail = Reader(_ => ""))

  def retryEither[B](f: F[Either[Throwable, B]]): ActionRetryEither[F, Unit, B] =
    retryEither[Unit, B](())(_ => f)

}
