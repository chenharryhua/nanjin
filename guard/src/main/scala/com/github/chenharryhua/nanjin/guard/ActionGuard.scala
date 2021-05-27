package com.github.chenharryhua.nanjin.guard

import cats.Functor
import cats.data.{Kleisli, Reader}
import cats.effect.{Async, Ref}
import cats.syntax.all._
import fs2.concurrent.Topic
import fs2.concurrent.Topic.Closed
import retry.RetryDetails.{GivingUp, WillDelayAndRetry}
import retry.{RetryDetails, RetryPolicies}

import java.util.UUID

final class RetryAction[F[_], A, B](
  topic: Topic[F, Event],
  serviceInfo: ServiceInfo,
  actionName: String,
  config: ActionConfig,
  input: A,
  kleisli: Kleisli[F, A, B],
  succ: Reader[(A, B), String],
  fail: Reader[(A, Throwable), String]
) {
  val params: ActionParams = config.evalConfig

  def withSuccInfo(succ: (A, B) => String): RetryAction[F, A, B] =
    new RetryAction[F, A, B](topic, serviceInfo, actionName, config.succOn, input, kleisli, Reader(succ.tupled), fail)

  def withFailInfo(fail: (A, Throwable) => String): RetryAction[F, A, B] =
    new RetryAction[F, A, B](topic, serviceInfo, actionName, config.failOn, input, kleisli, succ, Reader(fail.tupled))

  def run(implicit F: Async[F]): F[B] = Ref.of[F, Int](0).flatMap(ref => internalRun(ref))

  private def internalRun(ref: Ref[F, Int])(implicit F: Async[F]): F[B] = F.realTimeInstant.flatMap { ts =>
    val actionInfo: ActionInfo =
      ActionInfo(
        serviceInfo.applicationName,
        serviceInfo.serviceName,
        actionName,
        params.retryPolicy.policy[F].show,
        ts,
        params.alertMask,
        UUID.randomUUID())
    def onError(error: Throwable, details: RetryDetails): F[Unit] =
      details match {
        case wdr @ WillDelayAndRetry(_, _, _) =>
          topic
            .publish1(
              ActionRetrying(
                actionInfo = actionInfo,
                willDelayAndRetry = wdr,
                error = error
              ))
            .void <* ref.update(_ + 1)
        case gu @ GivingUp(_, _) =>
          topic
            .publish1(
              ActionFailed(
                actionInfo = actionInfo,
                givingUp = gu,
                notes = fail.run((input, error)),
                error = error
              ))
            .void
      }

    retry
      .retryingOnAllErrors[B](
        params.retryPolicy.policy[F].join(RetryPolicies.limitRetries(params.maxRetries)),
        onError)(kleisli.run(input))
      .flatTap(b =>
        ref.get.flatMap(count =>
          topic
            .publish1(
              ActionSucced(
                actionInfo = actionInfo,
                notes = succ.run((input, b)),
                numRetries = count
              ))
            .void))
  }
}

final class ActionGuard[F[_]](
  topic: Topic[F, Event],
  serviceInfo: ServiceInfo,
  actionName: String,
  config: ActionConfig) {

  def updateConfig(f: ActionConfig => ActionConfig): ActionGuard[F] =
    new ActionGuard[F](topic, serviceInfo, actionName, f(config))

  def retry[A, B](input: A)(f: A => F[B]): RetryAction[F, A, B] =
    new RetryAction[F, A, B](
      topic,
      serviceInfo,
      actionName,
      config,
      input,
      Kleisli(f),
      Reader(_ => ""),
      Reader(_ => ""))

  def retry[B](f: F[B]): RetryAction[F, Unit, B] = retry[Unit, B](())(_ => f)

  def fyi(msg: String)(implicit F: Functor[F]): F[Unit] =
    topic.publish1(ForYouInformation(serviceInfo.applicationName, msg)).void

}
