package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.{ConcurrentEffect, Sync, Timer}
import com.github.chenharryhua.nanjin.common.UpdateParams
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, NJConsumerRecord}
import frameless.{TypedDataset, TypedEncoder}
import org.apache.spark.sql.SparkSession
import cats.implicits._
import cats.effect.Concurrent

final case class SparKafkaSession(params: SparKafkaParams)(implicit val sparkSession: SparkSession)
    extends UpdateParams[SparKafkaParams, SparKafkaSession] {

  override def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSession =
    copy(params = f(params))

  def datasetFromKafka[F[_]: Concurrent, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[K, V]): F[TypedDataset[NJConsumerRecord[K, V]]] =
    SparKafka.datasetFromKafka[F, K, V](topic, params.timeRange, params.locationStrategy)

  def jsonFromKafka[F[_]: Concurrent, K, V](topic: => KafkaTopic[K, V]): F[TypedDataset[String]] =
    SparKafka.jsonDatasetFromKafka[F, K, V](topic, params.timeRange, params.locationStrategy)

  def datasetFromDisk[F[_]: Concurrent, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[K, V]): F[TypedDataset[NJConsumerRecord[K, V]]] =
    SparKafka.datasetFromDisk[F, K, V](topic, params.timeRange, params.rootPath)

  def saveToDisk[F[_]: Concurrent, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[K, V]): F[Unit] =
    SparKafka.saveToDisk[F, K, V](
      topic,
      params.timeRange,
      params.rootPath,
      params.saveMode,
      params.locationStrategy)

  def replay[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[K, V]): F[Unit] =
    SparKafka.replay(topic, params).map(_ => print(".")).compile.drain

  def sparkStream[F[_], K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[K, V]): TypedDataset[NJConsumerRecord[K, V]] =
    SparKafka.sparkStream(topic)

  def stats[F[_]: Concurrent, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[K, V]): F[Statistics[K, V]] =
    datasetFromKafka[F, K, V](topic).map(ds => Statistics(params, ds.dataset))
}

object SparKafkaSession {

  def default(implicit sparkSession: SparkSession): SparKafkaSession =
    new SparKafkaSession(SparKafkaParams.default)(sparkSession)
}
