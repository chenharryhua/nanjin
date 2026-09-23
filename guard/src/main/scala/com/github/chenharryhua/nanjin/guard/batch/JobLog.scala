package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.common.logging.{LogEntry, LogLevel}
import io.circe.syntax.given
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils

/** Renders a completed job as JSON for the batch report.
  *
  * Security/privacy note: a job's produced value is the user's data. `JobLog.Succeeded`/`Unsatisfied` carry
  * that value (as the type parameter `A`), but the two renders treat it differently:
  *
  *   - `standalone` — the render the framework emits '''automatically''' (see `lifecycle.logCompleted`) —
  *     discards the value and shows only lifecycle facts (identity, took, outcome tag). On failure it does
  *     not repeat the exception message: `standalone` is emitted with the throwable as the log entry's cause,
  *     so the stacktrace already carries it. It takes no `Encoder[A]`, so a produced value can never reach
  *     the auto-emitted log.
  *   - `inBatch` — reached only when the user explicitly serializes a returned `BatchResult` — shows the
  *     value under `result` and, on failure, an abbreviated exception message under `error`; it
  *     correspondingly requires an `Encoder[A]`.
  *
  * So the `Encoder[A]` requirement lives solely on `inBatch` and the batch-result encoders, never on the
  * execution path: showing values is opt-in via serialization, never automatic. It surfaces on the
  * `QuasiBatch`/`ValueBatch` encoders, whose per-job entries render the produced `A`, and on the
  * `MonadicBatch` encoder, which renders its successful final `A`. Monadic per-job entries remain
  * `JobState[Unit]`, so `inBatch` is called at `Unit`; failed final values are not serialized because their
  * throwable belongs in the log entry's exception data (see `MonadicBatch`).
  */
sealed private trait JobLog[A] extends Product {
  // The JSON status key is derived from the case's name (`Succeeded` -> "succeeded", etc.). This couples the
  // wire format to the Scala type name: renaming a case here is a WIRE-FORMAT BREAK, not a wire-safe rename.
  // `JobLogRenderTest` pins each expected key as a literal string and is the guard against accidental drift.
  final val tag: String = this.productPrefix.toLowerCase

  def standalone: Json = this match {
    case JobLog.Kickoff(job)  => Json.obj(tag -> job.asJson)
    case JobLog.Canceled(job) => Json.obj(tag -> job.asJson)

    case JobLog.Succeeded(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        JobLog.TOOK -> Json.fromString(fmt.format(record.took))
      )

    case JobLog.Unsatisfied(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        JobLog.TOOK -> Json.fromString(fmt.format(record.took))
      )

    case JobLog.Nonfatal(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        JobLog.TOOK -> Json.fromString(fmt.format(record.took))
      )

    case JobLog.Critical(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        JobLog.TOOK -> Json.fromString(fmt.format(record.took))
      )
  }

  def inBatch(using Encoder[A]): Json = this match {
    case JobLog.Kickoff(_)  => Json.Null // should not happen
    case JobLog.Canceled(_) => Json.Null // should not happen

    case JobLog.Succeeded(record, result) =>
      Json.obj(
        record.job.nameEntry,
        tag -> Json.fromString(fmt.format(record.took)),
        JobLog.RESULT -> result.asJson
      ).dropEmptyValues.dropNullValues

    case JobLog.Unsatisfied(record, result) =>
      Json.obj(
        record.job.nameEntry,
        tag -> Json.fromString(fmt.format(record.took)),
        JobLog.RESULT -> result.asJson
      ).dropEmptyValues.dropNullValues

    case JobLog.Nonfatal(record, error) =>
      Json.obj(
        record.job.nameEntry,
        tag -> Json.fromString(fmt.format(record.took)),
        JobLog.ERROR ->
          Json.fromString(StringUtils.abbreviate(ExceptionUtils.getMessage(error), JobLog.ERROR_MAX))
      )

    case JobLog.Critical(record, error) =>
      Json.obj(
        record.job.nameEntry,
        tag -> Json.fromString(fmt.format(record.took)),
        JobLog.ERROR ->
          Json.fromString(StringUtils.abbreviate(ExceptionUtils.getMessage(error), JobLog.ERROR_MAX))
      )
  }
}

private object JobLog {
  // Per-job field keys: the keys a single job's `standalone`/`inBatch` render emits alongside its status tag.
  inline val TOOK = "took"
  inline val ERROR = "error"
  inline val RESULT = "result"

  // `inBatch` abbreviates a failed job's exception message to this many characters so a batch report stays
  // compact for UI display; the full throwable is still available via the log entry's cause/stacktrace.
  inline val ERROR_MAX = 60

  // Batch-level report keys: used only by the QuasiBatch/ValueBatch/MonadicBatch encoders in `data.scala`.
  // `PASSED`/`FAILED` are integer tallies, deliberately named distinctly from the per-job "succeeded" status
  // tag (the `Succeeded` case's key) so the two never collide in one report: those are counts, the tag
  // carries a took duration.
  inline val PASSED = "passed"
  inline val FAILED = "failed"
  inline val JOBS = "jobs"
  inline val SPENT = "spent"

  final case class Kickoff(job: Job) extends JobLog[Nothing]
  final case class Canceled(job: Job) extends JobLog[Nothing]
  final case class Succeeded[A](record: JobRecord, result: A) extends JobLog[A]
  final case class Unsatisfied[A](record: JobRecord, result: A) extends JobLog[A]
  final case class Nonfatal[A](record: JobRecord, error: Throwable) extends JobLog[A]
  final case class Critical[A](record: JobRecord, error: Throwable) extends JobLog[A]
}

/** Classifies a completed `JobState` into the matching `JobLog` case and log level.
  *
  *   - a thrown exception is `Nonfatal` (`Warn`) for a `Quasi` job, whose failure is retained rather than
  *     aborting the batch, and `Critical` (`Error`) for a `Value` job or a monadic job (`kind = None`), where
  *     an exception is fatal to the batch;
  *   - a produced value is `Succeeded` (`Good`) when it satisfied its post-condition, or `Unsatisfied`
  *     (`Warn`) when a retained `Right` result failed its predicate (for example quasi jobs and monadic
  *     predicates that do not short-circuit).
  *
  * The `Some(ex)` on the failing cases carries the throwable through to the log entry for downstream
  * rendering.
  */
private def toLogEntry[A](js: JobState[A]): LogEntry[JobLog[A]] =
  js.result match {
    case Left(ex) =>
      js.record.job.kind match {
        case Some(BatchKind.Quasi) =>
          LogEntry(JobLog.Nonfatal(js.record, ex), LogLevel.Warn, Some(ex))
        // Value jobs and monadic jobs (kind = None) both treat an exception as fatal to the batch.
        case Some(BatchKind.Value) | None =>
          LogEntry(JobLog.Critical(js.record, ex), LogLevel.Error, Some(ex))
      }
    case Right(a) =>
      if (js.record.succeeded)
        LogEntry(JobLog.Succeeded(js.record, a), LogLevel.Good, None)
      else
        LogEntry(JobLog.Unsatisfied(js.record, a), LogLevel.Warn, None)
  }
