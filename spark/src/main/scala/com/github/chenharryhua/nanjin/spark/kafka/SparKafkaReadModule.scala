package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.NJRddLoader
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import io.circe.{Decoder => JsonDecoder}

private[kafka] trait SparKafkaReadModule[F[_], K, V] {
  self: SparKafka[F, K, V] =>

  import self.topic.topicDef._

  final def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy)
      .map(new CrRdd[F, K, V](_, topic.topicName, cfg))

  final def fromDisk: CrRdd[F, K, V] =
    new CrRdd[F, K, V](
      sk.loadDiskRdd[K, V](params.replayPath(topic.topicName)),
      topic.topicName,
      cfg)

  private val loader = new NJRddLoader(sparkSession)

  object load {

    def avro(pathStr: String): CrRdd[F, K, V] =
      new CrRdd[F, K, V](loader.avro[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    def parquet(pathStr: String)(implicit ev: TypedEncoder[OptionalKV[K, V]]): CrRdd[F, K, V] =
      new CrRdd[F, K, V](loader.parquet[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    def jackson(pathStr: String): CrRdd[F, K, V] =
      new CrRdd[F, K, V](loader.jackson[OptionalKV[K, V]](pathStr), topic.topicName, cfg)

    def circe(pathStr: String)(implicit
      jsonKeyDecoder: JsonDecoder[K],
      jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
      new CrRdd[F, K, V](loader.circe[OptionalKV[K, V]](pathStr), topic.topicName, cfg)
  }
}
