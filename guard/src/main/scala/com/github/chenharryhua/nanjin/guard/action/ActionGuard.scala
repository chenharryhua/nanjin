package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import cats.data.{EitherT, Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard.alert.{DailySummaries, ForYouInformation, NJEvent}
import com.github.chenharryhua.nanjin.guard.config.ActionConfig
import fs2.concurrent.Channel

final class ActionGuard[F[_]](
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  serviceName: String,
  appName: String,
  actionConfig: ActionConfig) {

  def apply(actionName: String): ActionGuard[F] =
    new ActionGuard[F](dailySummaries, channel, actionName, serviceName, appName, actionConfig)

  def updateActionConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](dailySummaries, channel, actionName, serviceName, appName, f(actionConfig))

  def retry[A, B](input: A)(f: A => F[B]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      serviceName = serviceName,
      appName = appName,
      actionConfig = actionConfig,
      input = input,
      kleisli = Kleisli(f),
      succ = Reader(_ => ""),
      fail = Reader(_ => ""))

  def retry[B](fb: F[B]): ActionRetry[F, Unit, B] = retry[Unit, B](())(_ => fb)

  def fyi(msg: String)(implicit F: Functor[F]): F[Unit] = channel.send(ForYouInformation(msg)).void

  def retryEither[A, B](input: A)(f: A => F[Either[Throwable, B]]): ActionRetryEither[F, A, B] =
    new ActionRetryEither[F, A, B](
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      serviceName = serviceName,
      appName = appName,
      actionConfig = actionConfig,
      input = input,
      eitherT = EitherT(Kleisli(f)),
      succ = Reader(_ => ""),
      fail = Reader(_ => ""))

  def retryEither[B](feb: F[Either[Throwable, B]]): ActionRetryEither[F, Unit, B] =
    retryEither[Unit, B](())(_ => feb)

  // maximum retries
  def max(retries: Int): ActionGuard[F] = updateActionConfig(_.withMaxRetries(retries))

  // post good news
  def magpie[B](fb: F[B])(f: B => String)(implicit F: Async[F]): F[B] =
    updateActionConfig(_.withSuccAlertOn.withFailAlertOff).retry(fb).withSuccNotes((_, b) => f(b)).run

  // post bad news
  def croak[B](fb: F[B])(f: Throwable => String)(implicit F: Async[F]): F[B] =
    updateActionConfig(_.withSuccAlertOff.withFailAlertOn).retry(fb).withFailNotes((_, ex) => f(ex)).run

  // no news
  def quiet[B](fb: F[B])(implicit F: Async[F]): F[B] =
    updateActionConfig(_.withSuccAlertOff.withFailAlertOff).run(fb)

  // no notes at all
  def run[B](fb: F[B])(implicit F: Async[F]): F[B] = retry[B](fb).run

}
