package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.TickStatus
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import io.circe.Json

// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/07/asynch-exns.pdf
final class NJAction[F[_], IN, OUT] private[action] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  arrow: IN => F[OUT],
  transInput: Option[IN => Json],
  transOutput: Option[(IN, OUT) => Json],
  transError: Kleisli[OptionT[F, *], (IN, Throwable), Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Async[F]) { self =>
  private def copy(
    transInput: Option[IN => Json] = self.transInput,
    transOutput: Option[(IN, OUT) => Json] = self.transOutput,
    transError: Kleisli[OptionT[F, *], (IN, Throwable), Json] = self.transError,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction[F, IN, OUT] =
    new NJAction[F, IN, OUT](
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      actionParams = self.actionParams,
      arrow = self.arrow,
      transInput = transInput,
      transOutput = transOutput,
      transError = transError,
      isWorthRetry = isWorthRetry
    )

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction[F, IN, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction[F, IN, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logInput(f: IN => Json): NJAction[F, IN, OUT]         = copy(transInput = Some(f))
  def logOutput(f: (IN, OUT) => Json): NJAction[F, IN, OUT] = copy(transOutput = Some(f))
  def logErrorM(f: (IN, Throwable) => F[Json]): NJAction[F, IN, OUT] =
    copy(transError = Kleisli((a: (IN, Throwable)) => OptionT(f(a._1, a._2).map(_.some))))
  def logError(f: (IN, Throwable) => Json): NJAction[F, IN, OUT] =
    logErrorM((a: IN, b: Throwable) => F.pure(f(a, b)))

  private[this] lazy val actionRunner: ReTry[F, IN, OUT] =
    new ReTry[F, IN, OUT](
      metricRegistry = metricRegistry,
      actionParams = actionParams,
      channel = channel,
      zerothTickStatus =
        TickStatus(actionParams.serviceParams.zerothTick).renewPolicy(actionParams.retryPolicy),
      arrow = arrow,
      transInput = transInput,
      transOutput = transOutput,
      transError = transError,
      isWorthRetry = isWorthRetry
    )

  def run[A](a: A)(implicit ev: A =:= IN): F[OUT] =
    actionRunner.run(a)
  def run[A, B](a: A, b: B)(implicit ev: (A, B) =:= IN): F[OUT] =
    actionRunner.run((a, b))
  def run[A, B, C](a: A, b: B, c: C)(implicit ev: (A, B, C) =:= IN): F[OUT] =
    actionRunner.run((a, b, c))
  def run[A, B, C, D](a: A, b: B, c: C, d: D)(implicit ev: (A, B, C, D) =:= IN): F[OUT] =
    actionRunner.run((a, b, c, d))
  def run[A, B, C, D, E](a: A, b: B, c: C, d: D, e: E)(implicit ev: (A, B, C, D, E) =:= IN): F[OUT] =
    actionRunner.run((a, b, c, d, e))

  def asResource: Resource[F, NJActionR[F, IN, OUT]] =
    Resource.make(F.pure(actionRunner))(ar => F.delay(ar.unregister())).map(new NJActionR[F, IN, OUT](_))
}

final class NJActionR[F[_], IN, OUT](private val actionRunner: ReTry[F, IN, OUT]) extends AnyVal {
  def run[A](a: A)(implicit ev: A =:= IN): F[OUT] =
    actionRunner.run(a)
  def run[A, B](a: A, b: B)(implicit ev: (A, B) =:= IN): F[OUT] =
    actionRunner.run((a, b))
  def run[A, B, C](a: A, b: B, c: C)(implicit ev: (A, B, C) =:= IN): F[OUT] =
    actionRunner.run((a, b, c))
  def run[A, B, C, D](a: A, b: B, c: C, d: D)(implicit ev: (A, B, C, D) =:= IN): F[OUT] =
    actionRunner.run((a, b, c, d))
  def run[A, B, C, D, E](a: A, b: B, c: C, d: D, e: E)(implicit ev: (A, B, C, D, E) =:= IN): F[OUT] =
    actionRunner.run((a, b, c, d, e))
}

final class NJAction0[F[_], OUT] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  arrow: F[OUT],
  transInput: Option[Json],
  transOutput: Option[OUT => Json],
  transError: Kleisli[OptionT[F, *], Throwable, Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Async[F]) { self =>
  private def copy(
    transInput: Option[Json] = self.transInput,
    transOutput: Option[OUT => Json] = self.transOutput,
    transError: Kleisli[OptionT[F, *], Throwable, Json] = self.transError,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction0[F, OUT] =
    new NJAction0[F, OUT](
      metricRegistry = self.metricRegistry,
      channel = self.channel,
      actionParams = self.actionParams,
      arrow = self.arrow,
      transInput = transInput,
      transOutput = transOutput,
      transError = transError,
      isWorthRetry = isWorthRetry
    )

  def withWorthRetryM(f: Throwable => F[Boolean]): NJAction0[F, OUT] = copy(isWorthRetry = f)
  def withWorthRetry(f: Throwable => Boolean): NJAction0[F, OUT] =
    withWorthRetryM((ex: Throwable) => F.pure(f(ex)))

  def logInput(info: Json): NJAction0[F, OUT]      = copy(transInput = Some(info))
  def logOutput(f: OUT => Json): NJAction0[F, OUT] = copy(transOutput = Some(f))
  def logErrorM(f: Throwable => F[Json]): NJAction0[F, OUT] =
    copy(transError = Kleisli((a: Throwable) => OptionT(f(a).map(_.some))))
  def logError(f: Throwable => Json): NJAction0[F, OUT] = logErrorM((a: Throwable) => F.pure(f(a)))

  private lazy val njAction = new NJAction[F, Unit, OUT](
    metricRegistry = metricRegistry,
    channel = channel,
    actionParams = actionParams,
    arrow = _ => arrow,
    transInput = transInput.map(j => _ => j),
    transOutput = transOutput.map(f => (_, b: OUT) => f(b)),
    transError = transError.local(_._2),
    isWorthRetry = isWorthRetry
  )

  def run: F[OUT] = njAction.run(())
}
