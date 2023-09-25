package com.github.chenharryhua.nanjin.guard.action

import cats.data.{Kleisli, OptionT}
import cats.effect.kernel.Temporal
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
  zerothTickStatus: TickStatus,
  arrow: IN => F[OUT],
  transInput: Kleisli[Option, IN, Json],
  transOutput: Option[(IN, OUT) => Json],
  transError: Kleisli[OptionT[F, *], (IN, Throwable), Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Temporal[F]) { self =>
  private def copy(
    transInput: Kleisli[Option, IN, Json] = self.transInput,
    transOutput: Option[(IN, OUT) => Json] = self.transOutput,
    transError: Kleisli[OptionT[F, *], (IN, Throwable), Json] = self.transError,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction[F, IN, OUT] =
    new NJAction[F, IN, OUT](
      zerothTickStatus = self.zerothTickStatus,
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

  def logInput(f: IN => Json): NJAction[F, IN, OUT] = copy(transInput = Kleisli((a: IN) => Some(f(a))))
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
      zerothTickStatus = zerothTickStatus,
      arrow = arrow,
      transInput = transInput,
      transOutput = transOutput,
      transError = transError,
      isWorthRetry = isWorthRetry
    )

  def run(input: IN): F[OUT] = actionRunner.run(input)
}

final class NJAction0[F[_], OUT] private[guard] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  zerothTickStatus: TickStatus,
  arrow: F[OUT],
  transInput: Option[Json],
  transOutput: Option[OUT => Json],
  transError: Kleisli[OptionT[F, *], Throwable, Json],
  isWorthRetry: Throwable => F[Boolean])(implicit F: Temporal[F]) { self =>
  private def copy(
    transInput: Option[Json] = self.transInput,
    transOutput: Option[OUT => Json] = self.transOutput,
    transError: Kleisli[OptionT[F, *], Throwable, Json] = self.transError,
    isWorthRetry: Throwable => F[Boolean] = self.isWorthRetry): NJAction0[F, OUT] =
    new NJAction0[F, OUT](
      zerothTickStatus = self.zerothTickStatus,
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
    zerothTickStatus = zerothTickStatus,
    metricRegistry = metricRegistry,
    channel = channel,
    actionParams = actionParams,
    arrow = _ => arrow,
    transInput = Kleisli(_ => transInput),
    transOutput = transOutput.map(f => (_, b: OUT) => f(b)),
    transError = transError.local(_._2),
    isWorthRetry = isWorthRetry
  )

  def run: F[OUT] = njAction.run(())
}
