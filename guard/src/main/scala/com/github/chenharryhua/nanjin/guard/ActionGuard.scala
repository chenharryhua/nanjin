package com.github.chenharryhua.nanjin.guard

import cats.Applicative
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.Temporal
import cats.effect.std.Dispatcher
import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.{ActionRetry, ActionRetryUnit, QuasiSucc, QuasiSuccUnit}
import com.github.chenharryhua.nanjin.guard.alert.{
  DailySummaries,
  ForYourInformation,
  NJEvent,
  PassThrough,
  ServiceInfo
}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams}
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax._

import java.time.ZoneId

final class ActionGuard[F[_]](
  serviceInfo: ServiceInfo,
  dispatcher: Dispatcher[F],
  dailySummaries: Ref[F, DailySummaries],
  channel: Channel[F, NJEvent],
  actionName: String,
  actionConfig: ActionConfig)
    extends UpdateConfig[ActionConfig, ActionGuard[F]] {
  val params: ActionParams = actionConfig.evalConfig

  def apply(actionName: String): ActionGuard[F] =
    new ActionGuard[F](serviceInfo, dispatcher, dailySummaries, channel, actionName, actionConfig)

  override def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](serviceInfo, dispatcher, dailySummaries, channel, actionName, f(actionConfig))

  def retry[A, B](input: A)(f: A => F[B])(implicit F: Applicative[F]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = input,
      kfab = Kleisli(f),
      succ = Reader(_ => ""),
      fail = Reader(_ => ""),
      isWorthRetry = Reader(_ => true),
      postCondition = Reader(_ => true))

  def retry[B](fb: F[B])(implicit F: Applicative[F]): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      fb = fb,
      succ = Reader(_ => ""),
      fail = Reader(_ => ""),
      isWorthRetry = Reader(_ => true),
      postCondition = Reader(_ => true))

  def fyi(msg: String)(implicit F: Temporal[F]): F[Unit] =
    realZonedDateTime(params.serviceParams)
      .flatMap(ts => channel.send(ForYourInformation(timestamp = ts, message = msg, isError = false)))
      .void

  def unsafeFYI(msg: String)(implicit F: Temporal[F]): Unit =
    dispatcher.unsafeRunSync(fyi(msg))

  def reportError(msg: String)(implicit F: Temporal[F]): F[Unit] =
    for {
      ts <- realZonedDateTime(params.serviceParams)
      _ <- dailySummaries.update(_.incErrorReport)
      _ <- channel.send(ForYourInformation(timestamp = ts, message = msg, isError = true))
    } yield ()

  def unsafeReportError(msg: String)(implicit F: Temporal[F]): Unit =
    dispatcher.unsafeRunSync(reportError(msg))

  def passThrough[A: Encoder](a: A)(implicit F: Temporal[F]): F[Unit] =
    realZonedDateTime(params.serviceParams).flatMap(ts => channel.send(PassThrough(ts, a.asJson))).void

  def unsafePassThrough[A: Encoder](a: A)(implicit F: Temporal[F]): Unit =
    dispatcher.unsafeRunSync(passThrough(a))

  // maximum retries
  def max(retries: Int): ActionGuard[F] = updateConfig(_.max_retries(retries))

  // post good news
  def magpie[B](fb: F[B])(f: B => String)(implicit F: Async[F]): F[B] =
    updateConfig(_.slack_succ_on.slack_fail_off).retry(fb).withSuccNotes(f).run

  // post bad news
  def croak[B](fb: F[B])(f: Throwable => String)(implicit F: Async[F]): F[B] =
    updateConfig(_.slack_succ_off.slack_fail_on).retry(fb).withFailNotes(f).run

  def quietly[B](fb: F[B])(implicit F: Async[F]): F[B] =
    updateConfig(_.slack_succ_off.slack_fail_off).run(fb)

  def loudly[B](fb: F[B])(implicit F: Async[F]): F[B] =
    updateConfig(_.slack_succ_on.slack_fail_on).run(fb)

  def run[B](fb: F[B])(implicit F: Async[F]): F[B] = retry[B](fb).run

  def zoneId: ZoneId = params.serviceParams.taskParams.zoneId

  def quasi[T[_], A, B](ta: T[A])(f: A => F[B]): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      serviceInfo = serviceInfo,
      dailySummaries = dailySummaries,
      channel = channel,
      actionName = actionName,
      params = params,
      input = ta,
      kfab = Kleisli(f),
      succ = Reader(_ => ""),
      fail = Reader(_ => ""))

  def quasi[T[_], B](tfb: T[F[B]]): QuasiSuccUnit[F, T, B] = new QuasiSuccUnit[F, T, B](
    serviceInfo = serviceInfo,
    dailySummaries = dailySummaries,
    channel = channel,
    actionName = actionName,
    params = params,
    tfb = tfb,
    succ = Reader(_ => ""),
    fail = Reader(_ => ""))

  def quasi[B](bs: F[B]*): QuasiSuccUnit[F, List, B] = quasi[List, B](bs.toList)
}
