package com.github.chenharryhua.nanjin.spark.kafka

import cats.Functor
import com.github.chenharryhua.nanjin.common.NJFileFormat
import higherkindness.droste.Algebra
import higherkindness.droste.data.Fix

import scala.concurrent.duration.FiniteDuration

sealed trait SparKafkaParamF[A]

object SparKafkaParamF {
  final case class WithDefault[K]() extends SparKafkaParamF[K]
  final case class WithBatchSize[K](value: Int, cont: K) extends SparKafkaParamF[K]
  final case class WithDuration[K](value: FiniteDuration, cont: K) extends SparKafkaParamF[K]
  final case class WithFileFormat[K](value: NJFileFormat, cont: K) extends SparKafkaParamF[K]

  implicit val SparKafkaParamFunctor: Functor[SparKafkaParamF] =
    cats.derived.semi.functor[SparKafkaParamF]

  type SparKafkaParam = Fix[SparKafkaParamF]

  val algebra: Algebra[SparKafkaParamF, SKParams] = Algebra[SparKafkaParamF, SKParams] {
    case WithDefault()        => SKParams.default
    case WithBatchSize(v, c)  => SKParams.uploadRate.composeLens(UploadRate.batchSize).set(v)(c)
    case WithDuration(v, c)   => SKParams.uploadRate.composeLens(UploadRate.duration).set(v)(c)
    case WithFileFormat(v, c) => SKParams.fileFormat.set(v)(c)
  }

  def withBatchSize(v: Int, cont: SparKafkaParam): SparKafkaParam =
    Fix(WithBatchSize(v, cont))

  def withDuration(v: FiniteDuration, cont: SparKafkaParam): SparKafkaParam =
    Fix(WithDuration(v, cont))

  def withFileFormat(ff: NJFileFormat, cont: SparKafkaParam): Fix[SparKafkaParamF] =
    Fix(WithFileFormat(ff, cont))

  def withJson(cont: SparKafkaParam): Fix[SparKafkaParamF] =
    withFileFormat(NJFileFormat.Json, cont)

  def withAvro(cont: SparKafkaParam): Fix[SparKafkaParamF] =
    withFileFormat(NJFileFormat.Avro, cont)

  def withParquet(cont: SparKafkaParam): Fix[SparKafkaParamF] =
    withFileFormat(NJFileFormat.Parquet, cont)

  def withJackson(cont: SparKafkaParam): Fix[SparKafkaParamF] =
    withFileFormat(NJFileFormat.Jackson, cont)

}
