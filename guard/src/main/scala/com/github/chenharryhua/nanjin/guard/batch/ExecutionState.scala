package com.github.chenharryhua.nanjin.guard.batch

import cats.Monad
import cats.data.{Kleisli, NonEmptyList, StateT}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import io.circe.Json
import monocle.Focus.focus
import monocle.Optional
import monocle.function.Index.index
import monocle.std.option.some

import scala.concurrent.duration.FiniteDuration

/** Threaded state for a monadic batch run: the current result-or-error together with the job history
  * accumulated so far.
  *
  * Each history entry is `Some(JobState[Json])` for a tracked job or `None` for an invisible
  * `pure`/`untracked` step. Tracked outcomes default to `Json.Null`; `renderOutcome` can attach a JSON
  * representation to the current tracked step. Invisible entries preserve positional history so `attempt` can
  * mark the most recent tracked job as handled. `history` is kept in reverse order (most recent step first)
  * and flattened after the run when building the final `MonadicBatch`. An `eoa` of `Left` means the chain has
  * short-circuited — either a job threw or a `withFilter` rejection happened — and no further jobs will run.
  *
  * @param eoa
  *   the accumulated result: `Right` while the chain is still succeeding, `Left` once a fatal error has
  *   short-circuited the chain
  * @param history
  *   tracked job states and invisible-step placeholders so far, most recent first
  */
final private case class ExecutionState[A](
  eoa: Either[Throwable, A],
  history: NonEmptyList[Option[JobState[Json]]]):

  /** Mark the chain as failed, replacing the result with `Left(ex)` while retaining the history. The `B` type
    * reflects that no value of the new type will be produced once the chain has short-circuited.
    */
  def update[B](ex: Throwable): ExecutionState[B] = copy(eoa = Left(ex))

  /** Fold a later segment `js` in front of this state: take the later segment's result, and prepend its
    * (already reversed) history onto this one, keeping the combined history most-recent-first.
    */
  def prependHistory[B](js: ExecutionState[B]): ExecutionState[B] =
    ExecutionState[B](js.eoa, js.history ::: history)

  /** Map over a still-succeeding result; a short-circuited (`Left`) state is left unchanged. */
  def map[B](f: A => B): ExecutionState[B] = copy(eoa = eoa.map(f))

  private val head: Optional[NonEmptyList[Option[JobState[Json]]], JobState[Json]] =
    index[NonEmptyList[Option[JobState[Json]]], Int, Option[JobState[Json]]](0)
      .andThen(some[JobState[Json]])

  def attempt: ExecutionState[Either[Throwable, A]] =
    ExecutionState[Either[Throwable, A]](
      Right(eoa),
      head.modify(_.focus(_.record.valid).modify(b => eoa.fold(_ => true, _ => b)))(history))

  def predicate(f: A => Boolean): ExecutionState[A] =
    copy(history = head.modify(_.focus(_.record.valid).modify(b => eoa.fold(_ => b, f)))(history))

  def renderOutcome(f: A => Json): ExecutionState[A] =
    copy(history = head.modify(_.focus(_.result).replace(eoa.map(f)))(history))

end ExecutionState

private object MonadicOps:
  private type State[F[_], R, A] = Kleisli[StateT[F, JobCursor, *], R, ExecutionState[A]]

  def flatMap[F[_]: Monad, R, A, B](first: State[F, R, A], next: A => State[F, R, B]): State[F, R, B] =
    Kleisli { context =>
      StateT { cursor =>
        first(context).run(cursor).flatMap { case (nextCursor, execState) =>
          execState.eoa match {
            case Left(ex)     => (nextCursor -> execState.update[B](ex)).pure[F]
            case Right(value) =>
              next(value)(context).run(nextCursor).map { case (finalCursor, nextState) =>
                finalCursor -> execState.prependHistory[B](nextState)
              }
          }
        }
      }
    }

  def map[F[_]: Monad, R, A, B](state: State[F, R, A], f: A => B): State[F, R, B] =
    state.map(_.map(f))

  def withFilter[F[_]: Monad, R, A](state: State[F, R, A], predicate: A => Boolean): State[F, R, A] =
    state.map { case unchanged @ ExecutionState(eoa, history) =>
      eoa match {
        case Left(_)      => unchanged
        case Right(value) =>
          if predicate(value) then unchanged
          else {
            val error = PostConditionUnsatisfied(history.head.map(_.record.job))
            ExecutionState[A](Left(error), history)
          }
      }
    }

  def attempt[F[_]: Monad, R, A](state: State[F, R, A]): State[F, R, Either[Throwable, A]] =
    state.map(_.attempt)

  def predicate[F[_]: Monad, R, A](state: State[F, R, A], f: A => Boolean): State[F, R, A] =
    state.map(_.predicate(f))

  def renderOutcome[F[_]: Monad, R, A](state: State[F, R, A], f: A => Json): State[F, R, A] =
    state.map(_.renderOutcome(f))

end MonadicOps

/** Threads the running job index together with the start time carried over from the previous job's `end`, so
  * each monadic job's `start` absorbs the gap left by invisible `untracked`/`pure` steps. See `JobRecord` for
  * the resulting per-job timing semantics.
  */
final private case class JobCursor(index: Int, start: FiniteDuration)
