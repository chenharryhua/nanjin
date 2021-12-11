package com.github.chenharryhua.nanjin.guard.action

import cats.data.Kleisli
import cats.effect.kernel.{Concurrent, Outcome}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import cats.{Alternative, Parallel, Traverse}
import com.github.chenharryhua.nanjin.guard.config.ActionParams
import com.github.chenharryhua.nanjin.guard.event.*

/** a group of actions which may fail individually but always success as a whole
  */
final class QuasiSucc[F[_], T[_], A, B] private[guard] (
  publisher: EventPublisher[F],
  params: ActionParams,
  ta: T[A],
  kfab: Kleisli[F, A, B])(implicit F: Concurrent[F]) {

  private def internal(eval: F[T[Either[(A, Throwable), (A, B)]]], runMode: RunMode)(implicit
    T: Traverse[T],
    L: Alternative[T]): F[T[B]] =
    for {
      actionInfo <- publisher.actionStart(params)
      res <- F
        .background(eval.map(_.partitionEither(identity)))
        .use(_.flatMap(_.embed(F.raiseError(ActionException.ActionCanceledInternally))))
        .guaranteeCase {
          case Outcome.Canceled() =>
            val error = ActionException.ActionCanceledExternally
            publisher.quasiFailed(actionInfo, params, error)
          case Outcome.Errored(error) =>
            publisher.quasiFailed(actionInfo, params, error)
          case Outcome.Succeeded(fb) =>
            publisher.quasiSucced(actionInfo, params, runMode, fb)
        }
    } yield T.map(res._2)(_._2)

  def seqRun(implicit T: Traverse[T], L: Alternative[T]): F[T[B]] =
    internal(ta.traverse(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Sequential)

  def parRun(implicit T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] =
    internal(ta.parTraverse(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Parallel)

  def parRun(n: Int)(implicit T: Traverse[T], L: Alternative[T]): F[T[B]] =
    internal(F.parTraverseN(n)(ta)(a => kfab.run(a).attempt.map(_.bimap((a, _), (a, _)))), RunMode.Parallel)

}

final class QuasiSuccUnit[F[_], T[_], B] private[guard] (
  publisher: EventPublisher[F],
  params: ActionParams,
  tfb: T[F[B]])(implicit F: Concurrent[F]) {

  private def toQuasiSucc: QuasiSucc[F, T, F[B], B] =
    new QuasiSucc[F, T, F[B], B](publisher = publisher, params = params, ta = tfb, kfab = Kleisli(identity))

  def seqRun(implicit T: Traverse[T], L: Alternative[T]): F[T[B]]                 = toQuasiSucc.seqRun
  def parRun(implicit T: Traverse[T], L: Alternative[T], P: Parallel[F]): F[T[B]] = toQuasiSucc.parRun
  def parRun(n: Int)(implicit T: Traverse[T], L: Alternative[T]): F[T[B]]         = toQuasiSucc.parRun(n)
}
