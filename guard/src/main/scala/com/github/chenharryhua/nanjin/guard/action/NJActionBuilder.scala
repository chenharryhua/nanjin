package com.github.chenharryhua.nanjin.guard.action

import cats.{Alternative, Endo, Traverse}
import cats.data.{Ior, Kleisli}
import cats.effect.kernel.Async
import cats.implicits.{
  catsSyntaxApplicativeError,
  toFoldableOps,
  toFunctorOps,
  toTraverseOps,
  toUnorderedFoldableOps
}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.ActionConfig
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel
import io.circe.Json
import io.circe.syntax.EncoderOps

import scala.concurrent.Future
import scala.util.control.NonFatal
import scala.util.Try

final class NJActionBuilder[F[_]](
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  name: String,
  actionConfig: ActionConfig
)(implicit F: Async[F])
    extends UpdateConfig[ActionConfig, NJActionBuilder[F]] {
  def apply(name: String): NJActionBuilder[F] =
    new NJActionBuilder[F](metricRegistry, channel, name, actionConfig)

  def updateConfig(f: Endo[ActionConfig]): NJActionBuilder[F] =
    new NJActionBuilder[F](metricRegistry, channel, name, f(actionConfig))

  // retries
  def retry[Z](fb: F[Z]): NJAction0[F, Z] = // 0 arity
    new NJAction0[F, Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(name),
      arrow = fb,
      transInput = F.pure(Json.Null),
      transOutput = _ => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, Z](f: A => F[Z]): NJAction[F, A, Z] =
    new NJAction[F, A, Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(name),
      arrow = f,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: A, _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, Z](f: (A, B) => F[Z]): NJAction[F, (A, B), Z] =
    new NJAction[F, (A, B), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(name),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, Z](f: (A, B, C) => F[Z]): NJAction[F, (A, B, C), Z] =
    new NJAction[F, (A, B, C), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(name),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): NJAction[F, (A, B, C, D), Z] =
    new NJAction[F, (A, B, C, D), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(name),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): NJAction[F, (A, B, C, D, E), Z] =
    new NJAction[F, (A, B, C, D, E), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(name),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D, E), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  // future

  def retryFuture[Z](future: F[Future[Z]]): NJAction0[F, Z] = // 0 arity
    retry(F.fromFuture(future))

  def retryFuture[A, Z](f: A => Future[Z]): NJAction[F, A, Z] =
    retry((a: A) => F.fromFuture(F.delay(f(a))))

  def retryFuture[A, B, Z](f: (A, B) => Future[Z]): NJAction[F, (A, B), Z] =
    retry((a: A, b: B) => F.fromFuture(F.delay(f(a, b))))

  def retryFuture[A, B, C, Z](f: (A, B, C) => Future[Z]): NJAction[F, (A, B, C), Z] =
    retry((a: A, b: B, c: C) => F.fromFuture(F.delay(f(a, b, c))))

  def retryFuture[A, B, C, D, Z](f: (A, B, C, D) => Future[Z]): NJAction[F, (A, B, C, D), Z] =
    retry((a: A, b: B, c: C, d: D) => F.fromFuture(F.delay(f(a, b, c, d))))

  def retryFuture[A, B, C, D, E, Z](f: (A, B, C, D, E) => Future[Z]): NJAction[F, (A, B, C, D, E), Z] =
    retry((a: A, b: B, c: C, d: D, e: E) => F.fromFuture(F.delay(f(a, b, c, d, e))))

  // error-like
  def retry[Z](t: Try[Z]): NJAction0[F, Z] = retry(F.fromTry(t))

  def retry[Z](e: Either[Throwable, Z]): NJAction0[F, Z] = retry(F.fromEither(e))

  // quasi never raise exception
  private def outputJson[G[_]: Traverse, Z](ior: Ior[G[Throwable], G[Z]], jobs: Long): Json = {
    val body = ior match {
      case Ior.Left(a)  => Json.obj("jobs" -> jobs.asJson, "failed" -> a.size.asJson)
      case Ior.Right(b) => Json.obj("jobs" -> jobs.asJson, "succed" -> b.size.asJson)
      case Ior.Both(a, b) =>
        Json.obj("jobs" -> jobs.asJson, "failed" -> a.size.asJson, "succed" -> b.size.asJson)
    }
    Json.obj("quasi" -> body)
  }

  private def inputJson(jobs: Long): Json =
    Json.obj("quasi" -> Json.obj("jobs" -> jobs.asJson))

  def quasi[G[_]: Traverse: Alternative, Z](gfz: G[F[Z]]): NJAction0[F, Ior[G[Throwable], G[Z]]] = {
    val jobs = gfz.size
    retry(gfz.traverse(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
      (fail.size, succ.size) match {
        case (0, _) => Ior.Right(succ)
        case (_, 0) => Ior.left(fail)
        case _      => Ior.Both(fail, succ)
      }
    }).logOutput(outputJson(_, jobs)).logInput(inputJson(jobs))
  }

  def quasi[Z](fzs: F[Z]*): NJAction0[F, Ior[List[Throwable], List[Z]]] = quasi[List, Z](fzs.toList)

  def quasi[G[_]: Traverse: Alternative, Z](
    parallelism: Int,
    gfz: G[F[Z]]): NJAction0[F, Ior[G[Throwable], G[Z]]] = {
    val jobs = gfz.size
    retry(
      F.parTraverseN(parallelism)(gfz)(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
        (fail.size, succ.size) match {
          case (0, _) => Ior.Right(succ)
          case (_, 0) => Ior.left(fail)
          case _      => Ior.Both(fail, succ)
        }
      }).logOutput(outputJson(_, jobs)).logInput(inputJson(jobs))
  }

  def quasi[Z](parallelism: Int)(fzs: F[Z]*): NJAction0[F, Ior[List[Throwable], List[Z]]] =
    quasi[List, Z](parallelism, fzs.toList)

}
