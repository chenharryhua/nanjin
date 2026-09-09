package com.github.chenharryhua.nanjin.guard.batch

import cats.syntax.all.catsSyntaxTuple2Semigroupal
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.jdk.DurationConverters.ScalaDurationOps

final private case class ExecutionState[A](eoa: Either[Throwable, A], history: List[CompletedJob]) {
  def update[B](ex: Throwable): ExecutionState[B] = copy(eoa = Left(ex))

  // reversed order
  def prependHistory[B](js: ExecutionState[B]): ExecutionState[B] =
    ExecutionState[B](js.eoa, js.history ::: history)

  def map[B](f: A => B): ExecutionState[B] = copy(eoa = eoa.map(f))
}

final private case class JobNameIndex[F[_], A](name: String, index: Int, fa: F[A])

private given [A: Encoder] => Encoder[Either[Throwable, A]] =
  Encoder.instance {
    case Left(ex)     => Json.fromString(ExceptionUtils.getMessage(ex))
    case Right(value) => value.asJson
  }

private def resultTag(succeeded: Boolean): String =
  if succeeded then "result" else "error"

private val SeverityNonFatal: "nonfatal" = "nonfatal"
private val SeverityCritical: "critical" = "critical"

// expects newest-first history (as accumulated by prependHistory)
private def monadicSpent(history: List[CompletedJob]): Duration =
  (history.headOption, history.lastOption)
    .mapN((last_end, first_start) => (last_end.end - first_start.start).toJava)
    .getOrElse(Duration.ZERO)

// expects chronological (oldest-first) history; rewrites each job's start to the
// previous job's end so per-job took absorbs the gap left by invisible lift/pure steps
private def monadicHistory(history: List[CompletedJob]): List[CompletedJob] =
  history match {
    case head :: next => head :: next.zip(history).map((job, prev_job) => job.copy(start = prev_job.end))
    case Nil          => Nil
  }
