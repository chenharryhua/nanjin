package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.UpdateParams
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession

final case class SparKafkaSession(params: SparKafkaParams)(implicit val sparkSession: SparkSession)
    extends UpdateParams[SparKafkaParams, SparKafkaSession] {

  def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSession =
    copy(params = f(params))

  def datasetFromKafka[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[ConsumerRecordDatasetWithParams[K, V]] =
    SparKafka
      .datasetFromKafka(topic, params.timeRange, params.locationStrategy)
      .map(tds => ConsumerRecordDatasetWithParams(this.params, tds.dataset))

  def datasetFromDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[ConsumerRecordDatasetWithParams[K, V]] =
    SparKafka
      .datasetFromDisk(topic, params.timeRange, params.rootPath)
      .map(tds => ConsumerRecordDatasetWithParams(this.params, tds.dataset))

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
    SparKafka
      .replay(
        topic,
        params.timeRange,
        params.rootPath,
        params.conversionTactics,
        params.uploadRate,
        params.clock,
        params.repartition)
      .map(_ => print("."))
      .compile
      .drain

  def sparkStream[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): TypedDataset[SparKafkaConsumerRecord[K, V]] =
    SparKafka.sparkStream(topic)
}

object SparKafkaSession {

  def default(implicit sparkSession: SparkSession): SparKafkaSession =
    new SparKafkaSession(SparKafkaParams.default)(sparkSession)
}
