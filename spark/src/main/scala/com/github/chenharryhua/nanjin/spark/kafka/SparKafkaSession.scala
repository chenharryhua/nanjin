package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, Sync, Timer}
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, NJConsumerRecord}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import cats.implicits._

final case class SparKafkaSession(params: SparKafkaParams)(implicit val sparkSession: SparkSession)
    extends UpdateParams[SparKafkaParams, SparKafkaSession] {

  override def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSession =
    copy(params = f(params))

  def datasetFromKafka[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[TypedDataset[NJConsumerRecord[K, V]]] =
    SparKafka.datasetFromKafka(topic, params.timeRange, params.locationStrategy)

  def jsonFromKafka[F[_]: Sync, K, V](topic: => KafkaTopic[F, K, V]): F[TypedDataset[String]] =
    SparKafka.jsonDatasetFromKafka(topic, params.timeRange, params.locationStrategy)

  def datasetFromDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[TypedDataset[NJConsumerRecord[K, V]]] =
    SparKafka.datasetFromDisk(topic, params.timeRange, params.rootPath)

  def saveToDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[Unit] =
    SparKafka.saveToDisk(
      topic,
      params.timeRange,
      params.rootPath,
      params.saveMode,
      params.locationStrategy)

  def replay[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[Unit] =
    SparKafka.replay(topic, params).map(_ => print(".")).compile.drain

  def sparkStream[F[_], K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): TypedDataset[NJConsumerRecord[K, V]] =
    SparKafka.sparkStream(topic)

  def stats[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[Statistics[K, V]] =
    datasetFromKafka(topic).map(ds => Statistics(params, ds.dataset))
}

object SparKafkaSession {

  def default(implicit sparkSession: SparkSession): SparKafkaSession =
    new SparKafkaSession(SparKafkaParams.default)(sparkSession)
}
