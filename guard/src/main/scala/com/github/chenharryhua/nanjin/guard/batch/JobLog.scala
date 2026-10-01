package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import com.github.chenharryhua.nanjin.common.logging.{LogEntry, LogLevel, MDC}
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
  *   - `inBatch` — reached only when the user explicitly serializes a returned `BatchResult` — shows a
  *     successful value or abbreviated exception message under its outcome tag (`succeeded`, `unsatisfied`,
  *     `nonfatal`, or `critical`), with the duration under `took`; it correspondingly requires an
  *     `Encoder[A]`.
  *
  * So the `Encoder[A]` requirement lives solely on `inBatch` and the batch-result encoders, never on the
  * execution path: showing values is opt-in via serialization, never automatic. It surfaces on the
  * `QuasiBatch`/`ValueBatch` encoders, whose per-job entries render the produced `A`, and on the
  * `MonadicBatch` encoder, which renders its successful final `A` and per-job JSON supplied through
  * `renderOutcome` or `render`. Monadic per-job entries are `JobState[Json]`; untranslated entries retain
  * `Json.Null` under their outcome tag. Failed final values are not serialized because their throwable
  * belongs in the log entry's exception data (see `MonadicBatch`).
  */
sealed private trait JobLog[A] extends Product {
  // The JSON status key is derived from the case's name (`Succeeded` -> "succeeded", etc.). This couples the
  // wire format to the Scala type name: renaming a case here is a WIRE-FORMAT BREAK, not a wire-safe rename.
  // `JobLogRenderTest` pins each expected key as a literal string and is the guard against accidental drift.
  final val tag: String = this.productPrefix.toLowerCase
  private inline val ERROR_MAX = 60 // chars
  private inline val TOOK = "took"

  def standalone: Json = this match {
    case JobLog.Kickoff(job)  => Json.obj(tag -> job.asJson)
    case JobLog.Canceled(job) => Json.obj(tag -> job.asJson)

    case JobLog.Succeeded(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        TOOK -> Json.fromString(fmt.format(record.took))
      )

    case JobLog.Unsatisfied(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        TOOK -> Json.fromString(fmt.format(record.took))
      )

    case JobLog.Nonfatal(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        TOOK -> Json.fromString(fmt.format(record.took))
      )

    case JobLog.Critical(record, _) =>
      Json.obj(
        tag -> record.job.asJson,
        TOOK -> Json.fromString(fmt.format(record.took))
      )
  }

  def inBatch(using Encoder[A]): Json = this match {
    case JobLog.Kickoff(_)  => Json.Null // should not happen
    case JobLog.Canceled(_) => Json.Null // should not happen

    case JobLog.Succeeded(record, result) =>
      Json.obj(
        record.job.nameEntry,
        TOOK -> Json.fromString(fmt.format(record.took)),
        tag -> result.asJson
      )

    case JobLog.Unsatisfied(record, result) =>
      Json.obj(
        record.job.nameEntry,
        TOOK -> Json.fromString(fmt.format(record.took)),
        tag -> result.asJson
      )

    case JobLog.Nonfatal(record, error) =>
      Json.obj(
        record.job.nameEntry,
        TOOK -> Json.fromString(fmt.format(record.took)),
        tag ->
          Json.fromString(StringUtils.abbreviate(ExceptionUtils.getMessage(error), ERROR_MAX))
      )

    case JobLog.Critical(record, error) =>
      Json.obj(
        record.job.nameEntry,
        TOOK -> Json.fromString(fmt.format(record.took)),
        tag ->
          Json.fromString(StringUtils.abbreviate(ExceptionUtils.getMessage(error), ERROR_MAX))
      )
  }
}

private object JobLog {
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
  *   - a `Left` result is `Nonfatal` (`Warn`) for a `Quasi` job, whose failure is retained rather than
  *     aborting the batch, and `Critical` (`Error`) for a `Value` job, where it is fatal to the batch. For a
  *     monadic job (`kind = None`) the flag decides: `JobFlag.Failed` is `Critical`, and any other flag (a
  *     failure caught by chain-level `attempt`, possibly reclassified by a later `predicate`) is `Nonfatal`;
  *   - a `Right` result is `Succeeded` (`Good`) when `JobState.succeeded` holds, or `Unsatisfied` (`Warn`)
  *     otherwise, i.e. when a retained value failed its predicate (for example quasi jobs and monadic
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
          LogEntry(JobLog.Nonfatal(js.record, ex), LogLevel.Warn, Some(ex), MDC.empty)
        // Value jobs are always fatal; a monadic exception is fatal only when flagged `Failed` (one caught by
        // chain-level `attempt` is flagged `Succeeded` and is nonfatal).
        case Some(BatchKind.Value) =>
          LogEntry(JobLog.Critical(js.record, ex), LogLevel.Error, Some(ex), MDC.empty)
        case None =>
          js.flag match {
            case JobFlag.Failed =>
              LogEntry(JobLog.Critical(js.record, ex), LogLevel.Error, Some(ex), MDC.empty)
            case _ => LogEntry(JobLog.Nonfatal(js.record, ex), LogLevel.Warn, Some(ex), MDC.empty)
          }
      }
    case Right(a) =>
      if (js.succeeded)
        LogEntry(JobLog.Succeeded(js.record, a), LogLevel.Good, None, MDC.empty)
      else
        LogEntry(JobLog.Unsatisfied(js.record, a), LogLevel.Warn, None, MDC.empty)
  }
