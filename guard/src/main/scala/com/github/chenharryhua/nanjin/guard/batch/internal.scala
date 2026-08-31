package com.github.chenharryhua.nanjin.guard.batch

import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import org.apache.commons.lang3.exception.ExceptionUtils

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
