package com.github.chenharryhua.nanjin.guard.batch

import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

import java.time.Duration
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

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

private def resultTag(succeeded: Boolean): String =
  if succeeded then "result" else "error"

// expects newest-first history (as accumulated by prependHistory)
private def monadicSpent(history: List[JobRecord]): Duration =
  (history.headOption, history.lastOption)
    .mapN((last_end, first_start) => (last_end.end - first_start.start).toJava)
    .getOrElse(Duration.ZERO)
