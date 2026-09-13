package com.github.chenharryhua.nanjin.guard.batch

import scala.concurrent.duration.FiniteDuration

/** Threaded state for a monadic batch run: the current result-or-error together with the completed job states
  * accumulated so far.
  *
  * Each history entry is a `JobState[Unit]`: the per-step produced value is erased to `Unit` because monadic
  * intermediate values are never rendered, so only the record and outcome are retained. `history` is kept in
  * reverse order (most recent job first) so that prepending a later segment is a cheap list cons; the batch
  * runner reverses it once when building the final `MonadicBatch`. An `eoa` of `Left` means the chain has
  * short-circuited — either a job threw or a `withFilter` rejection happened — and no further jobs will run.
  *
  * @param eoa
  *   the accumulated result: `Right` while the chain is still succeeding, `Left` once a fatal error has
  *   short-circuited the chain
  * @param history
  *   the completed job states (value erased to `Unit`) so far, most recent first
  */
final private case class ExecutionState[A](eoa: Either[Throwable, A], history: List[JobState[Unit]]):

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
end ExecutionState

/** A job that has not yet run: its display name, 1-based position in the batch, and the effect to execute. */
final private case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])

/** Threads the running job index together with the start time carried over from the previous job's `end`, so
  * each monadic job's `start` absorbs the gap left by invisible `untracked`/`pure` steps. See `JobRecord` for
  * the resulting per-job timing semantics.
  */
final private case class JobCursor(index: Int, start: FiniteDuration)
