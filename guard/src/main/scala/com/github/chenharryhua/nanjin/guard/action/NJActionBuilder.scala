package com.github.chenharryhua.nanjin.guard.action

import cats.{Alternative, Endo, Eval, Traverse}
import cats.data.{Ior, Validated}
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
import com.github.chenharryhua.nanjin.guard.policies
import cron4s.CronExpr
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryPolicy

import scala.concurrent.Future
import scala.util.Try

final class NJActionBuilder[F[_]](
  actionName: String,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionConfig: ActionConfig,
  retryPolicy: RetryPolicy[F]
)(implicit F: Async[F])
    extends UpdateConfig[ActionConfig, NJActionBuilder[F]] {
  private def copy(
    actionName: String = actionName,
    actionConfig: ActionConfig = actionConfig,
    retryPolicy: RetryPolicy[F] = retryPolicy
  ): NJActionBuilder[F] =
    new NJActionBuilder[F](actionName, metricRegistry, channel, actionConfig, retryPolicy)

  def updateConfig(f: Endo[ActionConfig]): NJActionBuilder[F] = copy(actionConfig = f(actionConfig))
  def apply(name: String): NJActionBuilder[F]                 = copy(actionName = name)
  def withRetryPolicy(rp: RetryPolicy[F]): NJActionBuilder[F] = copy(retryPolicy = rp)

  def withRetryPolicy(cronExpr: CronExpr, f: Endo[RetryPolicy[F]] = identity): NJActionBuilder[F] =
    withRetryPolicy(f(policies.cronBackoff[F](cronExpr, actionConfig.serviceParams.taskParams.zoneId)))

  private val alwaysRetry: Throwable => F[Boolean] = (_: Throwable) => F.pure(true)

  // retries
  def retry[Z](fb: F[Z]): NJAction0[F, Z] = // 0 arity
    new NJAction0[F, Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = fb,
      transInput = F.pure(Json.Null),
      transOutput = _ => F.pure(Json.Null),
      isWorthRetry = alwaysRetry
    )

  def retry[A, Z](f: A => F[Z]): NJAction[F, A, Z] =
    new NJAction[F, A, Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: A, _: Z) => F.pure(Json.Null),
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, Z](f: (A, B) => F[Z]): NJAction[F, (A, B), Z] =
    new NJAction[F, (A, B), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B), _: Z) => F.pure(Json.Null),
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, C, Z](f: (A, B, C) => F[Z]): NJAction[F, (A, B, C), Z] =
    new NJAction[F, (A, B, C), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C), _: Z) => F.pure(Json.Null),
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): NJAction[F, (A, B, C, D), Z] =
    new NJAction[F, (A, B, C, D), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D), _: Z) => F.pure(Json.Null),
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): NJAction[F, (A, B, C, D, E), Z] =
    new NJAction[F, (A, B, C, D, E), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D, E), _: Z) => F.pure(Json.Null),
      isWorthRetry = alwaysRetry
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
  def retry[Z](t: Try[Z]): NJAction0[F, Z]               = retry(F.fromTry(t))
  def retry[Z](e: Either[Throwable, Z]): NJAction0[F, Z] = retry(F.fromEither(e))
  def retry[Z](o: Option[Z]): NJAction0[F, Z] = retry(F.fromOption(o, new Exception("fail on None")))
  def retry[Z](v: Validated[Throwable, Z]): NJAction0[F, Z] = retry(F.fromValidated(v))
  def retry[Z](e: Eval[Z]): NJAction0[F, Z]                 = retry(F.catchNonFatalEval(e))

  // quasi
  private def mode(par: Option[Int]): (String, Json) =
    "mode" -> Json.fromString(par.fold("sequential")(p => s"parallel-$p"))
  private def jobs(size: Long): (String, Json) = "jobs" -> Json.fromLong(size)
  private def succ(size: Long): (String, Json) = "succed" -> Json.fromLong(size)
  private def fail(size: Long): (String, Json) = "failed" -> Json.fromLong(size)

  private def outputJson[G[_]: Traverse, Z](
    ior: Ior[G[Throwable], G[Z]],
    size: Long,
    parallelism: Option[Int]): Json = {
    val body = ior match {
      case Ior.Left(a)    => Json.obj(jobs(size), mode(parallelism), fail(a.size))
      case Ior.Right(b)   => Json.obj(jobs(size), mode(parallelism), succ(b.size))
      case Ior.Both(a, b) => Json.obj(jobs(size), mode(parallelism), fail(a.size), succ(b.size))
    }
    Json.obj("quasi" -> body)
  }

  private def inputJson(size: Long, par: Option[Int]): Json =
    Json.obj("quasi" -> Json.obj(jobs(size), mode(par)))

  // seq quasi
  def quasi[G[_]: Traverse: Alternative, Z](gfz: G[F[Z]]): NJAction0[F, Ior[G[Throwable], G[Z]]] = {
    val size = gfz.size
    retry(gfz.traverse(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
      (fail.size, succ.size) match {
        case (0, _) => Ior.Right(succ)
        case (_, 0) => Ior.left(fail)
        case _      => Ior.Both(fail, succ)
      }
    }).logOutput(outputJson(_, size, None)).logInput(inputJson(size, None))
  }

  def quasi[Z](fzs: F[Z]*): NJAction0[F, Ior[List[Throwable], List[Z]]] = quasi[List, Z](fzs.toList)

  // par quasi
  def parQuasi[G[_]: Traverse: Alternative, Z](
    parallelism: Int,
    gfz: G[F[Z]]): NJAction0[F, Ior[G[Throwable], G[Z]]] = {
    val size = gfz.size
    retry(
      F.parTraverseN(parallelism)(gfz)(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
        (fail.size, succ.size) match {
          case (0, _) => Ior.Right(succ)
          case (_, 0) => Ior.left(fail)
          case _      => Ior.Both(fail, succ)
        }
      }).logOutput(outputJson(_, size, Some(parallelism))).logInput(inputJson(size, Some(parallelism)))
  }

  def parQuasi[Z](parallelism: Int)(fzs: F[Z]*): NJAction0[F, Ior[List[Throwable], List[Z]]] =
    parQuasi[List, Z](parallelism, fzs.toList)

  def parQuasi[Z](lfz: List[F[Z]]): NJAction0[F, Ior[List[Throwable], List[Z]]] =
    parQuasi[List, Z](lfz.size, lfz)
  def parQuasi[Z](fzs: F[Z]*): NJAction0[F, Ior[List[Throwable], List[Z]]] =
    parQuasi[Z](fzs.toList)
}
