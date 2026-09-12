package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter as fmt
import io.circe.syntax.given
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

/** Renders a completed job as JSON for the batch report.
  *
  * Security/privacy note: a job's produced value is the user's data. `JobLog.Succeeded`/`Unsatisfied` carry
  * that value (as the type parameter `A`), but the two renders treat it differently:
  *
  *   - `standalone` — the render the framework emits '''automatically''' (see `logCompleted`) — discards the
  *     value and shows only lifecycle facts (identity, took, outcome tag, and, on failure, the exception
  *     message). It takes no `Encoder[A]`, so a produced value can never reach the auto-emitted log.
  *   - `inBatch` — reached only when the user explicitly serializes a returned `BatchResult` — shows the
  *     value under `result`, and correspondingly requires an `Encoder[A]`.
  *
  * So the `Encoder[A]` requirement lives solely on `inBatch` (hence on the `BatchResult` encoders), never on
  * the execution path: showing the value is opt-in via serialization, never automatic.
  */
sealed private trait JobLog[A] {

  def standalone: Json = this match {
    case JobLog.Kickoff(job)  => Json.obj(JobLog.KICKOFF -> job.asJson)
    case JobLog.Canceled(job) => Json.obj(JobLog.CANCELED -> job.asJson)

    case JobLog.Succeeded(record, _) =>
      Json.obj(JobLog.SUCCEEDED -> Json.fromString(fmt.format(record.took)))
        .deepMerge(record.job.asJson)

    case JobLog.Unsatisfied(record, _) =>
      Json.obj(JobLog.UNSATISFIED -> Json.fromString(fmt.format(record.took)))
        .deepMerge(record.job.asJson)

    case JobLog.Nonfatal(record, error) =>
      Json.obj(
        JobLog.NONFATAL -> Json.fromString(fmt.format(record.took)),
        JobLog.ERROR -> Json.fromString(ExceptionUtils.getMessage(error))
      ).deepMerge(record.job.asJson)

    case JobLog.Critical(record, error) =>
      Json.obj(
        JobLog.CRITICAL -> Json.fromString(fmt.format(record.took)),
        JobLog.ERROR -> Json.fromString(ExceptionUtils.getMessage(error))
      ).deepMerge(record.job.asJson)
  }

  def inBatch(using Encoder[A]): Json = this match {
    case JobLog.Kickoff(_)  => Json.Null // should not happen
    case JobLog.Canceled(_) => Json.Null // should not happen

    case JobLog.Succeeded(record, result) =>
      Json.obj(
        record.job.nameEntry,
        JobLog.SUCCEEDED -> Json.fromString(fmt.format(record.took)),
        JobLog.RESULT -> result.asJson).dropEmptyValues.dropNullValues

    case JobLog.Unsatisfied(record, result) =>
      Json.obj(
        record.job.nameEntry,
        JobLog.UNSATISFIED -> Json.fromString(fmt.format(record.took)),
        JobLog.RESULT -> result.asJson).dropEmptyValues.dropNullValues

    case JobLog.Nonfatal(record, error) =>
      Json.obj(
        record.job.nameEntry,
        JobLog.NONFATAL -> Json.fromString(fmt.format(record.took)),
        JobLog.ERROR -> Json.fromString(ExceptionUtils.getMessage(error))
      )

    case JobLog.Critical(record, error) =>
      Json.obj(
        record.job.nameEntry,
        JobLog.CRITICAL -> Json.fromString(fmt.format(record.took)),
        JobLog.ERROR -> Json.fromString(ExceptionUtils.getMessage(error))
      )
  }
}

private object JobLog {
  val SUCCEEDED = "succeeded"
  val UNSATISFIED = "unsatisfied"
  val NONFATAL = "nonfatal"
  val CRITICAL = "critical"
  val KICKOFF = "kickoff"
  val CANCELED = "canceled"
  val ERROR = "error"
  val RESULT = "result"

  final case class Kickoff(job: Job) extends JobLog[Nothing]
  final case class Canceled(job: Job) extends JobLog[Nothing]
  final case class Succeeded[A](record: JobRecord, result: A) extends JobLog[A]
  final case class Unsatisfied[A](record: JobRecord, result: A) extends JobLog[A]
  final case class Nonfatal[A](record: JobRecord, error: Throwable) extends JobLog[A]
  final case class Critical[A](record: JobRecord, error: Throwable) extends JobLog[A]
}
