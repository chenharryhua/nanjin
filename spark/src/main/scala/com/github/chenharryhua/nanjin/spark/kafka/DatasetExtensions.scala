package com.github.chenharryhua.nanjin.spark.kafka

import java.time.ZoneId

import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.loaders
import frameless.{TypedDataset, TypedEncoder}
import io.circe.{Decoder => JsonDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

private[kafka] trait DatasetExtensions {

  implicit final class SparKafkaTopicSyntax[F[_], K, V](topic: KafkaTopic[F, K, V])
      extends Serializable {

    def sparKafka(cfg: SKConfig)(implicit ss: SparkSession): SparKafka[F, K, V] =
      new SparKafka(topic, ss, cfg)

    def sparKafka(zoneId: ZoneId)(implicit ss: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(topic.topicDef.topicName, zoneId))

    def sparKafka(dtr: NJDateTimeRange)(implicit ss: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(topic.topicDef.topicName, dtr))

    def sparKafka(implicit ss: SparkSession): SparKafka[F, K, V] =
      sparKafka(SKConfig(topic.topicDef.topicName, ZoneId.systemDefault()))

  }

  implicit final class TopicDefExt[K, V](topicDef: TopicDef[K, V]) extends Serializable {
    implicit private val keyCodec: AvroCodec[K] = topicDef.serdeOfKey.avroCodec
    implicit private val valCodec: AvroCodec[V] = topicDef.serdeOfVal.avroCodec

    object load {

      private val codec: AvroCodec[OptionalKV[K, V]] = shapeless.cachedImplicit

      def avro(pathStr: String)(implicit
        keyEncoder: TypedEncoder[K],
        valEncoder: TypedEncoder[V],
        ss: SparkSession): TypedDataset[OptionalKV[K, V]] = {
        val ate: AvroTypedEncoder[OptionalKV[K, V]] = AvroTypedEncoder(codec)
        loaders.avro[OptionalKV[K, V]](pathStr, ate)(ss)
      }

      def parquet(pathStr: String)(implicit
        keyEncoder: TypedEncoder[K],
        valEncoder: TypedEncoder[V],
        ss: SparkSession): TypedDataset[OptionalKV[K, V]] = {
        val ate: AvroTypedEncoder[OptionalKV[K, V]] = AvroTypedEncoder(codec)
        loaders.parquet[OptionalKV[K, V]](pathStr, ate)(ss)
      }

      def json(pathStr: String)(implicit
        keyEncoder: TypedEncoder[K],
        valEncoder: TypedEncoder[V],
        ss: SparkSession): TypedDataset[OptionalKV[K, V]] = {
        val ate: AvroTypedEncoder[OptionalKV[K, V]] = AvroTypedEncoder(codec)
        loaders.json[OptionalKV[K, V]](pathStr, ate)(ss)
      }

      object rdd {

        def avro(pathStr: String)(implicit ss: SparkSession): RDD[OptionalKV[K, V]] =
          loaders.rdd.avro[OptionalKV[K, V]](pathStr, codec)

        def jackson(pathStr: String)(implicit ss: SparkSession): RDD[OptionalKV[K, V]] =
          loaders.rdd.jackson[OptionalKV[K, V]](pathStr, codec)

        def binAvro(pathStr: String)(implicit ss: SparkSession): RDD[OptionalKV[K, V]] =
          loaders.rdd.binAvro[OptionalKV[K, V]](pathStr, codec)

        def circe(pathStr: String)(implicit
          ev: JsonDecoder[OptionalKV[K, V]],
          ss: SparkSession): RDD[OptionalKV[K, V]] =
          loaders.rdd.circe[OptionalKV[K, V]](pathStr)
      }
    }
  }
}
