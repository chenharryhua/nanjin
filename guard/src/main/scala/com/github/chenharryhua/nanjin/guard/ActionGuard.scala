package com.github.chenharryhua.nanjin.guard

import cats.collections.Predicate
import cats.data.{Kleisli, Reader}
import cats.effect.Temporal
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.{ActionRetry, ActionRetryUnit, QuasiSucc, QuasiSuccUnit}
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, ActionParams}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream
import io.circe.Encoder
import io.circe.syntax.*

import java.time.ZoneId

final class ActionGuard[F[_]] private[guard] (
  publisher: EventPublisher[F],
  dispatcher: Dispatcher[F],
  actionConfig: ActionConfig)(implicit F: Temporal[F])
    extends UpdateConfig[ActionConfig, ActionGuard[F]] {

  val params: ActionParams = actionConfig.evalConfig

  override def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](publisher, dispatcher, f(actionConfig))

  def apply(actionName: String): ActionGuard[F] = updateConfig(_.withActionName(actionName))

  def trivial: ActionGuard[F] = updateConfig(_.withTrivial)
  def notice: ActionGuard[F]  = updateConfig(_.withNotice)

  def retry[A, B](f: A => F[B]): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](
      publisher = publisher,
      params = params,
      kfab = Kleisli(f),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")),
      isWorthRetry = Reader(_ => true),
      postCondition = Predicate(_ => true))

  def retry[B](fb: F[B]): ActionRetryUnit[F, B] =
    new ActionRetryUnit[F, B](
      fb = fb,
      publisher = publisher,
      params = params,
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")),
      isWorthRetry = Reader(_ => true),
      postCondition = Predicate(_ => true))

  def run[B](fb: F[B]): F[B] = retry(fb).run

  def passThrough[A: Encoder](a: A): F[Unit]    = publisher.passThrough(params, a.asJson)
  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))

  def count(n: Long): F[Unit]    = publisher.count(params, n)
  def unsafeCount(n: Long): Unit = dispatcher.unsafeRunSync(count(n))

  // maximum retries
  def max(retries: Int): ActionGuard[F] = updateConfig(_.withMaxRetries(retries))

  def nonStop[B](fb: F[B]): F[Nothing] =
    apply("nonStop").trivial
      .updateConfig(_.withNonTermination.withMaxRetries(0))
      .retry(fb)
      .run
      .flatMap[Nothing](_ => F.raiseError(new Exception("never happen")))

  def nonStop[B](sfb: Stream[F, B]): F[Nothing] = nonStop(sfb.compile.drain)

  val zoneId: ZoneId = params.serviceParams.taskParams.zoneId

  def quasi[T[_], A, B](ta: T[A])(f: A => F[B]): QuasiSucc[F, T, A, B] =
    new QuasiSucc[F, T, A, B](
      publisher = publisher,
      params = params,
      ta = ta,
      kfab = Kleisli(f),
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")))

  def quasi[T[_], B](tfb: T[F[B]]): QuasiSuccUnit[F, T, B] =
    new QuasiSuccUnit[F, T, B](
      publisher = publisher,
      params = params,
      tfb = tfb,
      succ = Kleisli(_ => F.pure("")),
      fail = Kleisli(_ => F.pure("")))

  def quasi[B](bs: F[B]*): QuasiSuccUnit[F, List, B] = quasi[List, B](bs.toList)
}
