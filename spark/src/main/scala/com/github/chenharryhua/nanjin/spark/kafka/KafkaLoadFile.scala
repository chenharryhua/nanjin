package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.TypedEncoder
import io.circe.{Decoder => JsonDecoder}
import org.apache.spark.sql.SparkSession

final class KafkaLoadFile[F[_], K, V] private[kafka] (klf: SparKafka[F, K, V])
    extends Serializable {

  def avro(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.avro[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  def parquet(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.parquet[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  def json(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.json[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  def jackson(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.jackson[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  def binAvro(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.binAvro[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  def circe(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ev: JsonDecoder[OptionalKV[K, V]],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.circe[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  def objectFile(pathStr: String)(implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V],
    ss: SparkSession): CrDS[F, K, V] =
    klf.crDS(loaders.objectFile[OptionalKV[K, V]](pathStr, OptionalKV.ate(klf.topic.topicDef)))

  object rdd {

    def avro(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(
        loaders.rdd
          .avro[OptionalKV[K, V]](pathStr, OptionalKV.avroCodec(klf.topic.topicDef).avroDecoder))

    def jackson(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(
        loaders.rdd
          .jackson[OptionalKV[K, V]](pathStr, OptionalKV.avroCodec(klf.topic.topicDef).avroDecoder))

    def binAvro(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(
        loaders.rdd
          .binAvro[OptionalKV[K, V]](pathStr, OptionalKV.avroCodec(klf.topic.topicDef).avroDecoder))

    def circe(pathStr: String)(implicit
      ev: JsonDecoder[OptionalKV[K, V]],
      ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.circe[OptionalKV[K, V]](pathStr))

    def objectFile(pathStr: String)(implicit ss: SparkSession): CrRdd[F, K, V] =
      klf.crRdd(loaders.rdd.objectFile[OptionalKV[K, V]](pathStr))
  }
}
