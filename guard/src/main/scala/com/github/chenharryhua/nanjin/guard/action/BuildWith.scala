package com.github.chenharryhua.nanjin.guard.action

import cats.Endo
import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.config.{MetricName, PublishStrategy, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.concurrent.Channel
import io.circe.Json

final class BuildWith[F[_]: Async, IN, OUT] private[action] (
  metricName: MetricName,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  arrow: Kleisli[F, IN, OUT]) {

  private val init: BuildWith.Builder[F, IN, OUT] =
    new BuildWith.Builder[F, IN, OUT](
      policy = Policy.giveUp,
      publishStrategy = PublishStrategy.Silent,
      transInput = Reader((_: IN) => Json.Null),
      transOutput = Reader((_: (IN, OUT)) => Json.Null),
      transError = Reader[(IN, Throwable), Json](_ => Json.Null),
      isWorthRetry = Reader[(IN, Throwable), Boolean](_ => true)
    )

  def buildWith(f: Endo[BuildWith.Builder[F, IN, OUT]]): Resource[F, Kleisli[F, IN, OUT]] =
    f(init).build(metricName, channel, serviceParams, arrow)
}

object BuildWith {
  final class Builder[F[_]: Async, IN, OUT] private[action] (
    policy: Policy,
    publishStrategy: PublishStrategy,
    transInput: Reader[IN, Json],
    transOutput: Reader[(IN, OUT), Json],
    transError: Reader[(IN, Throwable), Json],
    isWorthRetry: Reader[(IN, Throwable), Boolean]
  ) {
    private def copy(
      policy: Policy = policy,
      publishStrategy: PublishStrategy = publishStrategy,
      transInput: Reader[IN, Json] = transInput,
      transOutput: Reader[(IN, OUT), Json] = transOutput,
      transError: Reader[(IN, Throwable), Json] = transError,
      isWorthRetry: Reader[(IN, Throwable), Boolean] = isWorthRetry
    ): Builder[F, IN, OUT] =
      new Builder[F, IN, OUT](
        policy = policy,
        publishStrategy = publishStrategy,
        transInput = transInput,
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = isWorthRetry)

    def tapInput(f: IN => Json): Builder[F, IN, OUT] =
      copy(transInput = Reader[IN, Json](f))

    def tapOutput(f: (IN, OUT) => Json): Builder[F, IN, OUT] =
      copy(transOutput = Reader[(IN, OUT), Json](f.tupled))

    def tapError(f: (IN, Throwable) => Json): Builder[F, IN, OUT] =
      copy(transError = Reader[(IN, Throwable), Json](f.tupled))

    def worthRetry(f: (IN, Throwable) => Boolean): Builder[F, IN, OUT] =
      copy(isWorthRetry = Reader[(IN, Throwable), Boolean](f.tupled))

    def worthRetry(f: Throwable => Boolean): Builder[F, IN, OUT] =
      copy(isWorthRetry = Reader(f).local((tup: (IN, Throwable)) => tup._2))

    def withPublishStrategy(f: PublishStrategy.type => PublishStrategy): Builder[F, IN, OUT] =
      copy(publishStrategy = f(PublishStrategy))

    def withPolicy(policy: Policy): Builder[F, IN, OUT] =
      copy(policy = policy)

    private[action] def build(
      metricName: MetricName,
      channel: Channel[F, NJEvent],
      serviceParams: ServiceParams,
      arrow: Kleisli[F, IN, OUT]): Resource[F, Kleisli[F, IN, OUT]] =
      ReTry(
        publishStrategy = publishStrategy,
        channel = channel,
        serviceParams = serviceParams,
        metricName = metricName,
        zerothTickStatus = serviceParams.initialStatus.renewPolicy(policy),
        arrow = arrow,
        transInput = transInput,
        transOutput = transOutput,
        transError = transError,
        isWorthRetry = isWorthRetry
      )
  }
}
