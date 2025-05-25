package com.github.chenharryhua.nanjin.guard

import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import com.github.chenharryhua.nanjin.guard.translator.decimalFormatter
import io.circe.Json
import squants.Dimensionless
import squants.information.{DataRate, Information}
import squants.time.{Frequency, Nanoseconds}

import java.time.Duration

package object batch {
  def jsonDataRate(took: Duration, number: Information): Json = {
    val count: String = s"${decimalFormatter.format(number.value.toLong)} ${number.unit.symbol}"

    val speed: DataRate   = number / Nanoseconds(took.toNanos)
    val formatted: String = s"${decimalFormatter.format(speed.value.toLong)} ${speed.unit.symbol}"

    Json.obj("volume" -> Json.fromString(count), "speed" -> Json.fromString(formatted))
  }

  def jsonScalarRate(took: Duration, number: Dimensionless): Json = {
    val count: String   = s"${decimalFormatter.format(number.value.toLong)} ${number.unit.symbol}"
    val rate: Frequency = number / Nanoseconds(took.toNanos)
    val ratio: Double   = number.value / number.toEach
    val formatted: String =
      s"${decimalFormatter.format((rate.toHertz * ratio).toLong)} ${number.unit.symbol}/s"

    Json.obj("count" -> Json.fromString(count), "rate" -> Json.fromString(formatted))
  }

  private[batch] def sequentialBatchResultState[F[_]](metrics: Metrics[F], mode: BatchMode)(
    results: List[JobResultState]
  ): BatchResultState =
    BatchResultState(
      label = metrics.metricLabel,
      spent = results.map(_.took).foldLeft(Duration.ZERO)(_ plus _),
      mode = mode,
      jobs = results
    )

  private[batch] def sequentialBatchResultValue[F[_], A](metrics: Metrics[F], mode: BatchMode)(
    results: List[JobResultValue[A]]): BatchResultValue[List[A]] = {
    val brs = sequentialBatchResultState(metrics, mode)(results.map(_.resultState))
    val as  = results.map(_.value)
    BatchResultValue(brs, as)
  }

}
