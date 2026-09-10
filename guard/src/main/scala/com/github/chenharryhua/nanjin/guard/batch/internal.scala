package com.github.chenharryhua.nanjin.guard.batch

import com.github.chenharryhua.nanjin.common.logging.{LogEntry, LogLevel}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import scala.concurrent.duration.FiniteDuration

final private case class ExecutionState[A](eoa: Either[Throwable, A], history: List[JobRecord]) {
  def update[B](ex: Throwable): ExecutionState[B] = copy(eoa = Left(ex))

  // reversed order
  def prependHistory[B](js: ExecutionState[B]): ExecutionState[B] =
    ExecutionState[B](js.eoa, js.history ::: history)

  def map[B](f: A => B): ExecutionState[B] = copy(eoa = eoa.map(f))
}

final private case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])

// threads the running job index together with the start time carried over from the previous job's end,
// so each monadic job's start absorbs the gap left by invisible untracked/pure steps
final private case class JobCursor(index: Int, start: FiniteDuration)

private given [A: Encoder] => Encoder[Either[Throwable, A]] =
  Encoder.instance {
    case Left(ex)     => Json.fromString(ExceptionUtils.getMessage(ex))
    case Right(value) => value.asJson
  }

// JSON object keys shared by the JobLog renderings and the batch-report encoders
private object JsonKeys {
  val TOOK = "took"
  val RESULT = "result"
  val ERROR = "error"
  val FAILED = "failed"
  val SUCCEEDED = "succeeded"
  val UNSATISFIED = "unsatisfied"
  val NONFATAL = "nonfatal"
  val CRITICAL = "critical"
  val KICKOFF = "kickoff"
  val CANCELED = "canceled"
}

private def toLogEntry[A: Encoder](js: JobState[A]): LogEntry[JobLog] =
  js.result match {
    case Left(ex) =>
      js.record.job.kind match {
        case BatchKind.Quasi => LogEntry(JobLog.Nonfatal(js.record, ex), LogLevel.Warn, Some(ex))
        case BatchKind.Value => LogEntry(JobLog.Critical(js.record, ex), LogLevel.Error, Some(ex))
      }
    case Right(a) =>
      if (js.succeeded)
        LogEntry(JobLog.Succeeded(js.record, a.asJson), LogLevel.Good, None)
      else
        LogEntry(JobLog.Unsatisfied(js.record, a.asJson), LogLevel.Warn, None)
  }
