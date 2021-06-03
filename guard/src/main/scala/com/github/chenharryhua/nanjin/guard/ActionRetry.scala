package com.github.chenharryhua.nanjin.guard

import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.concurrent.Topic
import retry.RetryPolicies

import java.util.UUID

final class ActionRetry[F[_], A, B](
  topic: Topic[F, NJEvent],
  serviceInfo: ServiceInfo,
  actionName: String,
  config: ActionConfig,
  input: A,
  kleisli: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]) {
  val params: ActionParams = config.evalConfig

  def withSuccInfo(succ: (A, B) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](topic, serviceInfo, actionName, config, input, kleisli, Reader(succ.tupled), fail)

  def withFailInfo(fail: (A, Throwable) => String): ActionRetry[F, A, B] =
    new ActionRetry[F, A, B](topic, serviceInfo, actionName, config, input, kleisli, succ, Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(internalRun)

  private def internalRun(ref: Ref[F, Int])(implicit F: Async[F]): F[B] = F.realTimeInstant.flatMap { ts =>
    val actionInfo: ActionInfo =
      ActionInfo(
        applicationName = serviceInfo.applicationName,
        serviceName = serviceInfo.serviceName,
        actionName = actionName,
        params = params,
        id = UUID.randomUUID(),
        launchTime = ts
      )

    val base = new ActionRetryBase[F, A, B](input, succ, fail)

    retry
      .retryingOnAllErrors[B](
        params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
        base.onError(actionInfo, topic, ref))(kleisli.run(input))
      .flatTap(b =>
        for {
          count <- ref.get
          now <- F.realTimeInstant
          _ <- topic.publish1(ActionSucced(actionInfo, now, count, base.succNotes(b)))
        } yield ())
  }
}
