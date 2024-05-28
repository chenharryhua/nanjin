package com.github.chenharryhua.nanjin.guard.action

import cats.Endo
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Resource}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import io.circe.Json

final class BuildWith[F[_]: Async, IN, OUT] private[action] (
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionParams: ActionParams,
  arrow: Kleisli[F, IN, OUT]) {

  def buildWith(f: Endo[BuildWith.Builder[F, IN, OUT]]): Resource[F, Kleisli[F, IN, OUT]] = {
    val init = new BuildWith.Builder[F, IN, OUT](
      transInput = Reader((_: IN) => Json.Null),
      transOutput = Reader((_: (IN, OUT)) => Json.Null),
      transError = Reader((_: IN) => Json.Null),
      isWorthRetry = Reader((_: Throwable) => true)
    )

    f(init).build(metricRegistry, channel, actionParams, arrow)
  }
}

object BuildWith {
  final class Builder[F[_]: Async, IN, OUT] private[action] (
    transInput: Reader[IN, Json],
    transOutput: Reader[(IN, OUT), Json],
    transError: Reader[IN, Json],
    isWorthRetry: Reader[Throwable, Boolean]
  ) {
    def tapInput(f: IN => Json): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](
        transInput = Reader[IN, Json](f),
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = isWorthRetry)

    def tapOutput(f: (IN, OUT) => Json): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](
        transInput = transInput,
        transOutput = Reader[(IN, OUT), Json](a => f(a._1, a._2)),
        transError = transError,
        isWorthRetry = isWorthRetry)

    def tapError(f: IN => Json): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](
        transInput = transInput,
        transOutput = transOutput,
        transError = Reader[IN, Json](f),
        isWorthRetry = isWorthRetry)

    def worthRetry(f: Throwable => Boolean): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](
        transInput = transInput,
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = Reader[Throwable, Boolean](f))

    private[action] def build(
      metricRegistry: MetricRegistry,
      channel: Channel[F, NJEvent],
      actionParams: ActionParams,
      arrow: Kleisli[F, IN, OUT]): Resource[F, Kleisli[F, IN, OUT]] =
      ReTry(
        metricRegistry = metricRegistry,
        channel = channel,
        actionParams = actionParams,
        arrow = arrow,
        transInput = transInput,
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = isWorthRetry
      )
  }
}
