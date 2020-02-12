package com.github.chenharryhua.nanjin.spark.kafka

import cats.Functor
import higherkindness.droste.data.Fix

import scala.concurrent.duration.FiniteDuration

trait SparKafkaParamF[A]

object SparKafkaParamF {
  final case class End() extends SparKafkaParamF[Nothing]
  final case class WithBatchSize[K](value: Int, cont: K) extends SparKafkaParamF[K]
  final case class WithDuration[K](value: FiniteDuration, cont: K) extends SparKafkaParamF[K]

  implicit val SparKafkaParamFunctor: Functor[SparKafkaParamF] =
    cats.derived.semi.functor[SparKafkaParamF]

  type SparKafkaParam = Fix[SparKafkaParamF]

  def withBatchSize(v: Int, sum: SparKafkaParam): SparKafkaParam =
    Fix(WithBatchSize(v, sum))

  def withDuration(v: FiniteDuration, sum: SparKafkaParam): SparKafkaParam =
    Fix(WithDuration(v, sum))

}
