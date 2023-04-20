package com.github.chenharryhua.nanjin.guard.action

import cats.data.Ior
import cats.effect.kernel.Async
import cats.implicits.{
  catsSyntaxApplicativeError,
  toFoldableOps,
  toFunctorOps,
  toTraverseOps,
  toUnorderedFoldableOps
}
import cats.{Alternative, Endo, Traverse}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ActionConfig, Measurement}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.policies
import cron4s.CronExpr
import fs2.concurrent.Channel
import io.circe.Json
import retry.RetryPolicy

import scala.concurrent.Future

final class NJActionBuilder[F[_]](
  actionName: String,
  measurement: Measurement,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  actionConfig: ActionConfig,
  retryPolicy: RetryPolicy[F]
)(implicit F: Async[F])
    extends UpdateConfig[ActionConfig, NJActionBuilder[F]] { self =>
  private def copy(
    actionName: String = self.actionName,
    actionConfig: ActionConfig = self.actionConfig,
    retryPolicy: RetryPolicy[F] = self.retryPolicy
  ): NJActionBuilder[F] =
    new NJActionBuilder[F](actionName, measurement, metricRegistry, channel, actionConfig, retryPolicy)

  def updateConfig(f: Endo[ActionConfig]): NJActionBuilder[F] = copy(actionConfig = f(actionConfig))
  def apply(name: String): NJActionBuilder[F]                 = copy(actionName = name)

  def withRetryPolicy(rp: RetryPolicy[F]): NJActionBuilder[F] = copy(retryPolicy = rp)

  def withRetryPolicy(cronExpr: CronExpr, f: Endo[RetryPolicy[F]]): NJActionBuilder[F] =
    withRetryPolicy(f(policies.cronBackoff[F](cronExpr, actionConfig.serviceParams.taskParams.zoneId)))

  def withRetryPolicy(cronExpr: CronExpr): NJActionBuilder[F] =
    withRetryPolicy(policies.cronBackoff[F](cronExpr, actionConfig.serviceParams.taskParams.zoneId))

  private def alwaysRetry: Throwable => F[Boolean] = (_: Throwable) => F.pure(true)

  // retries
  def retry[Z](fz: F[Z]): NJAction0[F, Z] = // 0 arity
    new NJAction0[F, Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, measurement, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = fz,
      transInput = F.pure(Json.Null),
      transOutput = None,
      isWorthRetry = alwaysRetry
    )

  def delay[Z](z: => Z): NJAction0[F, Z] = retry(F.delay(z))

  def retry[A, Z](f: A => F[Z]): NJAction[F, A, Z] =
    new NJAction[F, A, Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, measurement, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f,
      transInput = _ => F.pure(Json.Null),
      transOutput = None,
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, Z](f: (A, B) => F[Z]): NJAction[F, (A, B), Z] =
    new NJAction[F, (A, B), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, measurement, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = None,
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, C, Z](f: (A, B, C) => F[Z]): NJAction[F, (A, B, C), Z] =
    new NJAction[F, (A, B, C), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, measurement, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = None,
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): NJAction[F, (A, B, C, D), Z] =
    new NJAction[F, (A, B, C, D), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, measurement, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = None,
      isWorthRetry = alwaysRetry
    )

  def retry[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): NJAction[F, (A, B, C, D, E), Z] =
    new NJAction[F, (A, B, C, D, E), Z](
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = actionConfig.evalConfig(actionName, measurement, retryPolicy.show),
      retryPolicy = retryPolicy,
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = None,
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

  // quasi
  private def mode(par: Option[Int]): (String, Json) =
    "mode" -> Json.fromString(par.fold("sequential")(p => s"parallel-$p"))
  private def jobs(size: Long): (String, Json) = "jobs" -> Json.fromLong(size)
  private def done(size: Long): (String, Json) = "done" -> Json.fromLong(size)
  private def fail(size: Long): (String, Json) = "failed" -> Json.fromLong(size)

  private def outputJson[G[_]: Traverse, Z](
    ior: Ior[G[Throwable], G[Z]],
    size: Long,
    parallelism: Option[Int]): Json = {
    val body = ior match {
      case Ior.Left(a)    => Json.obj(jobs(size), mode(parallelism), fail(a.size))
      case Ior.Right(b)   => Json.obj(jobs(size), mode(parallelism), done(b.size))
      case Ior.Both(a, b) => Json.obj(jobs(size), mode(parallelism), fail(a.size), done(b.size))
    }
    Json.obj("quasi" -> body)
  }

  // in case cancelled
  private def inputJson(size: Long, par: Option[Int]): Json =
    Json.obj("quasi" -> Json.obj(jobs(size), mode(par)))

  // seq quasi
  def quasi[G[_]: Traverse: Alternative, Z](gfz: G[F[Z]]): NJAction0[F, Ior[G[Throwable], G[Z]]] = {
    val size = gfz.size
    retry(gfz.traverse(_.attempt).map(_.partitionEither(identity)).map { case (fail, done) =>
      (fail.size, done.size) match {
        case (0, _) => Ior.Right(done) // success if no error
        case (_, 0) => Ior.left(fail) // failure if no success
        case _      => Ior.Both(fail, done) // quasi
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
      F.parTraverseN(parallelism)(gfz)(_.attempt).map(_.partitionEither(identity)).map { case (fail, done) =>
        (fail.size, done.size) match {
          case (0, _) => Ior.Right(done)
          case (_, 0) => Ior.left(fail)
          case _      => Ior.Both(fail, done)
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
