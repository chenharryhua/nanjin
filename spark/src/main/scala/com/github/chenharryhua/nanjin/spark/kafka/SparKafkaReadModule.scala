package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.RddLoadFromFile
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import io.circe.{Decoder => JsonDecoder}

private[kafka] trait SparKafkaReadModule[F[_], K, V] {
  self: SparKafka[F, K, V] =>

  import self.topic.topicDef._

  final def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.unloadKafka(topic, params.timeRange, params.locationStrategy)
      .map(new CrRdd[F, K, V](_, topic.topicName, cfg))

  final def fromDisk: CrRdd[F, K, V] =
    new CrRdd[F, K, V](
      sk.loadDiskRdd[K, V](params.replayPath(topic.topicName)),
      topic.topicName,
      cfg)

  private val delegate = new RddLoadFromFile(sparkSession)

  object read {

    final def avro(pathStr: String): CrRdd[F, K, V] =
      new CrRdd[F, K, V](delegate.avro[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    final def parquet(pathStr: String): CrRdd[F, K, V] =
      new CrRdd[F, K, V](delegate.parquet[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    final def jackson(pathStr: String): CrRdd[F, K, V] =
      new CrRdd[F, K, V](delegate.jackson[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    final def circe(pathStr: String)(implicit
      jsonKeyDecoder: JsonDecoder[K],
      jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
      new CrRdd[F, K, V](delegate.circe[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    final val single: SingleFile = new SingleFile
    final val multi: MultiFile   = new MultiFile

    final class SingleFile {

      def avro: CrRdd[F, K, V] =
        read.avro(params.pathBuilder(topic.topicName, NJFileFormat.Avro))

      def jackson: CrRdd[F, K, V] =
        read.jackson(params.pathBuilder(topic.topicName, NJFileFormat.Jackson))

      def parquet(implicit k: TypedEncoder[K], v: TypedEncoder[V]): CrRdd[F, K, V] =
        read.parquet(params.pathBuilder(topic.topicName, NJFileFormat.Parquet))

      def circe(implicit
        jsonKeyDecoder: JsonDecoder[K],
        jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
        read.circe(params.pathBuilder(topic.topicName, NJFileFormat.CirceJson))
    }

    final class MultiFile {

      def avro: CrRdd[F, K, V] =
        read.avro(params.pathBuilder(topic.topicName, NJFileFormat.MultiAvro))

      def jackson: CrRdd[F, K, V] =
        read.jackson(params.pathBuilder(topic.topicName, NJFileFormat.MultiJackson))

      def parquet: CrRdd[F, K, V] =
        read.parquet(params.pathBuilder(topic.topicName, NJFileFormat.MultiParquet))

      def circe(implicit
        jsonKeyDecoder: JsonDecoder[K],
        jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
        read.circe(params.pathBuilder(topic.topicName, NJFileFormat.MultiCirce))
    }
  }
}
