package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.logging.{LogEntry, LogLevel}

import scala.concurrent.duration.FiniteDuration

/** Threaded state for a monadic batch run: the current result-or-error together with the `JobRecord`s
  * accumulated so far.
  *
  * `history` is kept in reverse order (most recent job first) so that prepending a later segment is a cheap
  * list cons; the batch runner reverses it once when building the final `MonadicBatch`. An `eoa` of `Left`
  * means the chain has short-circuited — either a job threw or a `withFilter`/predicate failed — and no
  * further jobs will run.
  *
  * @param eoa
  *   the accumulated result: `Right` while the chain is still succeeding, `Left` once it has failed
  * @param history
  *   the completed job records so far, most recent first
  */
final private case class ExecutionState[A](eoa: Either[Throwable, A], history: List[JobState[Unit]]) {

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
}

/** A job that has not yet run: its display name, 1-based position in the batch, and the effect to execute. */
final private case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])

/** Threads the running job index together with the start time carried over from the previous job's `end`, so
  * each monadic job's `start` absorbs the gap left by invisible `untracked`/`pure` steps. See `JobRecord` for
  * the resulting per-job timing semantics.
  */
final private case class JobCursor(index: Int, start: FiniteDuration)

/** JSON object keys shared by the `JobLog` renderings and the batch-report encoders, kept in one place so the
  * per-job log entries and the aggregate `BatchResult` encoders stay in sync.
  */
private object JsonKeys {
  val SUCCEEDED = "succeeded"
  val UNSATISFIED = "unsatisfied"
  val NONFATAL = "nonfatal"
  val CRITICAL = "critical"
  val KICKOFF = "kickoff"
  val CANCELED = "canceled"
  val ERROR = "error"
  val RESULT = "result"
  // QuasiBatch per-outcome counts. Named distinctly from the per-job `SUCCEEDED` status tag so the two
  // never collide in one report: these are integer tallies, that tag carries a took duration.
  val PASSED = "passed"
  val FAILED = "failed"
}

/** Classifies a completed `JobState` into the matching `JobLog` case and log level.
  *
  *   - a thrown exception is `Nonfatal` (`Warn`) for a `Quasi` job, whose failure is retained rather than
  *     aborting the batch, and `Critical` (`Error`) for a `Value` job or a monadic job (`kind = None`), where
  *     an exception is fatal to the batch;
  *   - a produced value is `Succeeded` (`Good`) when it satisfied its post-condition, or `Unsatisfied`
  *     (`Warn`) when the predicate rejected it.
  *
  * The `Some(ex)` on the failing cases carries the throwable through to the log entry for downstream
  * rendering.
  */
private def toLogEntry[A](js: JobState[A]): LogEntry[JobLog] =
  js.result match {
    case Left(ex) =>
      js.record.job.kind match {
        case Some(BatchKind.Quasi) => LogEntry(JobLog.Nonfatal(js.record, ex), LogLevel.Warn, Some(ex))
        // Value jobs and monadic jobs (kind = None) both treat an exception as fatal to the batch.
        case Some(BatchKind.Value) | None =>
          LogEntry(JobLog.Critical(js.record, ex), LogLevel.Error, Some(ex))
      }
    case Right(_) =>
      if (js.succeeded)
        LogEntry(JobLog.Succeeded(js.record), LogLevel.Good, None)
      else
        LogEntry(JobLog.Unsatisfied(js.record), LogLevel.Warn, None)
  }
