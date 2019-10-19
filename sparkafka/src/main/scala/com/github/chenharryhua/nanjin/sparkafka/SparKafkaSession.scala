package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ConcurrentEffect, Resource, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark.SparkSettings
import frameless.TypedEncoder
import fs2.Stream
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final case class SparKafkaSession(params: SparKafkaParams)(
  implicit val sparkSession: SparkSession) {

  def update(f: SparKafkaParams => SparKafkaParams): SparKafkaSession =
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
        params.uploadRate)
      .map(_ => print("."))
      .compile
      .drain
}

@Lenses final case class SparKafkaSettings(sparkSettings: SparkSettings, params: SparKafkaParams) {

  def updateKafka(f: SparKafkaParams => SparKafkaParams): SparKafkaSettings =
    SparKafkaSettings.params.modify(f)(this)

  def updateSpark(f: SparkConf => SparkConf): SparKafkaSettings =
    SparKafkaSettings.sparkSettings.composeLens(SparkSettings.conf).modify(f)(this)

  def setLogLevel(logLevel: String): SparKafkaSettings =
    SparKafkaSettings.sparkSettings.composeLens(SparkSettings.logLevel).set(logLevel)(this)

  def session: SparKafkaSession = SparKafkaSession(params)(sparkSettings.session)

  def sessionResource[F[_]: Sync]: Resource[F, SparKafkaSession] =
    sparkSettings.sessionResource.map(SparKafkaSession(params)(_))

  def sessionStream[F[_]: Sync]: Stream[F, SparKafkaSession] =
    Stream.resource(sessionResource)
}

object SparKafkaSettings {
  val default: SparKafkaSettings = SparKafkaSettings(SparkSettings.default, SparKafkaParams.default)

  def apply(sparkSettings: SparkSettings): SparKafkaSettings =
    SparKafkaSettings(sparkSettings, SparKafkaParams.default)

  def apply(params: SparKafkaParams): SparKafkaSettings =
    SparKafkaSettings(SparkSettings.default, params)

}
