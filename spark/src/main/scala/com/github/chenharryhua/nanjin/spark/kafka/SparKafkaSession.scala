package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, Resource, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.{SparkSettings, UpdateParams}
import frameless.TypedEncoder
import fs2.Stream
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final case class SparKafkaSession(params: SparKafkaParams)(implicit val sparkSession: SparkSession)
    extends UpdateParams[SparKafkaParams, SparKafkaSession] {

  def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSession =
    copy(params = f(params))

  def datasetFromKafka[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[ConsumerRecordDatasetWithParams[K, V]] =
    SparKafka
      .datasetFromKafka(topic, params.timeRange, params.locationStrategy)
      .map(tds => ConsumerRecordDatasetWithParams(this.params, tds.dataset))

  def datasetFromDisk[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[ConsumerRecordDatasetWithParams[K, V]] =
    SparKafka
      .datasetFromDisk(topic, params.timeRange, params.rootPath)
      .map(tds => ConsumerRecordDatasetWithParams(this.params, tds.dataset))

  def saveToDisk[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
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
        params.conversionStrategy,
        params.uploadRate,
        params.clock)
      .map(_ => print("."))
      .compile
      .drain
}

object SparKafkaSession {

  def default(implicit sparkSession: SparkSession): SparKafkaSession =
    new SparKafkaSession(SparKafkaParams.default)(sparkSession)
}
