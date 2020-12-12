package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedEncoder
import io.circe.{Decoder => JsonDecoder}
import org.apache.spark.sql.SparkSession

final class KafkaLoadFile[F[_], K, V](klf: SparKafka[F, K, V]) extends Serializable {
  private val keyCodec: AvroCodec[K]                 = klf.topic.topicDef.serdeOfKey.avroCodec
  private val valCodec: AvroCodec[V]                 = klf.topic.topicDef.serdeOfVal.avroCodec
  private val avroCodec: AvroCodec[OptionalKV[K, V]] = OptionalKV.avroCodec(keyCodec, valCodec)

  private def ate(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): AvroTypedEncoder[OptionalKV[K, V]] = {
    implicit val te: TypedEncoder[OptionalKV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder[OptionalKV[K, V]](avroCodec)
  }

  def avro(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.avro[OptionalKV[K, V]](pathStr, ate)(ss), ate)

  def parquet(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.parquet[OptionalKV[K, V]](pathStr, ate)(ss), ate)

  def json(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.json[OptionalKV[K, V]](pathStr, ate)(ss), ate)

  def jackson(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.jackson[OptionalKV[K, V]](pathStr, ate)(ss), ate)

  def binAvro(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.binAvro[OptionalKV[K, V]](pathStr, ate)(ss), ate)

  def circe(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ev: JsonDecoder[OptionalKV[K, V]],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.circe[OptionalKV[K, V]](pathStr, ate), ate)

  def objectFile(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.objectFile[OptionalKV[K, V]](pathStr, ate), ate)

  object rdd {

    def avro(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.avro[OptionalKV[K, V]](pathStr, avroCodec.avroDecoder))

    def jackson(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.jackson[OptionalKV[K, V]](pathStr, avroCodec.avroDecoder))

    def binAvro(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.binAvro[OptionalKV[K, V]](pathStr, avroCodec.avroDecoder))

    def circe(pathStr: String)(implicit
      ev: JsonDecoder[OptionalKV[K, V]],
      ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.circe[OptionalKV[K, V]](pathStr))

    def objectFile(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.objectFile[OptionalKV[K, V]](pathStr))
  }
}
