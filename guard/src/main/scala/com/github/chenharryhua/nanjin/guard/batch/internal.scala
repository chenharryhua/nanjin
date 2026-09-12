package com.github.chenharryhua.nanjin.guard.batch

import cats.syntax.show.showInterpolator
import com.github.chenharryhua.nanjin.common.logging.{LogEntry, LogLevel}
import com.github.chenharryhua.nanjin.guard.metrics.MetricScope
import io.circe.Json

private object JsonKeys {
  // QuasiBatch per-outcome counts. Named distinctly from the per-job `SUCCEEDED` status tag so the two
  // never collide in one report: these are integer tallies, that tag carries a took duration.
  val PASSED = "passed"
  val FAILED = "failed"

  val JOBS = "jobs"
  val SPENT = "spent"
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

private def batchEntry(mode: BatchMode, kind: Option[BatchKind], scope: MetricScope): (String, Json) =
  kind.fold(show"$mode Batch" -> Json.fromString(scope.label.value))(k =>
    show"$mode $k Batch" -> Json.fromString(scope.label.value))
