package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.Sync
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import com.github.chenharryhua.nanjin.spark.NJRddLoader
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import io.circe.generic.auto._
import io.circe.{Decoder => JsonDecoder}

private[kafka] trait SparKafkaLoadModule[F[_], K, V] {
  self: SparKafka[F, K, V] =>
  import self.topic.topicDef._

  final def fromKafka(implicit sync: Sync[F]): F[CrRdd[F, K, V]] =
    sk.kafkaBatch(topic, params.timeRange, params.locationStrategy).map(crRdd)

  final def fromDisk: CrRdd[F, K, V] =
    crRdd(sk.loadDiskRdd[K, V](params.replayPath))

  private val loader = new NJRddLoader(sparkSession)

  object load {

    def avro(pathStr: String): CrRdd[F, K, V] =
      crRdd(loader.avro[OptionalKV[K, V]](pathStr))

    def avro: CrRdd[F, K, V] =
      avro(params.outPath(NJFileFormat.Avro))

    def binAvro(pathStr: String): CrRdd[F, K, V] =
      crRdd(loader.binAvro[OptionalKV[K, V]](pathStr))

    def binAvro: CrRdd[F, K, V] =
      binAvro(params.outPath(NJFileFormat.BinaryAvro))

    def parquet(pathStr: String)(implicit ev: TypedEncoder[OptionalKV[K, V]]): CrRdd[F, K, V] =
      crRdd(loader.parquet[OptionalKV[K, V]](pathStr))

    def parquet(implicit ev: TypedEncoder[OptionalKV[K, V]]): CrRdd[F, K, V] =
      parquet(params.outPath(NJFileFormat.Parquet))

    def jackson(pathStr: String): CrRdd[F, K, V] =
      crRdd(loader.jackson[OptionalKV[K, V]](pathStr))

    def jackson: CrRdd[F, K, V] =
      jackson(params.outPath(NJFileFormat.Jackson))

    def circe(pathStr: String)(implicit
      jsonKeyDecoder: JsonDecoder[K],
      jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
      crRdd(loader.circe[OptionalKV[K, V]](pathStr))

    def circe(implicit
      jsonKeyDecoder: JsonDecoder[K],
      jsonValDecoder: JsonDecoder[V]): CrRdd[F, K, V] =
      circe(params.outPath(NJFileFormat.Circe))
  }
}
