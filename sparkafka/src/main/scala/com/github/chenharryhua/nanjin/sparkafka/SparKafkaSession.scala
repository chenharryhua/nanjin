package com.github.chenharryhua.nanjin.sparkafka

import java.time.LocalDate

import cats.effect.{ConcurrentEffect, Resource, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaTimestamp, KafkaTopic}
import frameless.functions.aggregate.count
import frameless.{TypedDataset, TypedEncoder}
import fs2.Stream
import monocle.macros.Lenses
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

final case class ConsumerRecordDatasetWithSession[K: TypedEncoder, V: TypedEncoder](
  sparkafka: SparKafkaSession,
  consumerRecords: TypedDataset[SparKafkaConsumerRecord[K, V]]) {

  def minutely: TypedDataset[MinutelyAggResult] = {
    val minute: TypedDataset[Int] = consumerRecords.deserialized.map { m =>
      KafkaTimestamp(m.timestamp).local(sparkafka.params.zoneId).getMinute
    }
    val res = minute.groupBy(minute.asCol).agg(count(minute.asCol)).as[MinutelyAggResult]
    res.orderBy(res('minute).asc)
  }

  def hourly: TypedDataset[HourlyAggResult] = {
    val hour = consumerRecords.deserialized.map { m =>
      KafkaTimestamp(m.timestamp).local(sparkafka.params.zoneId).getHour
    }
    val res = hour.groupBy(hour.asCol).agg(count(hour.asCol)).as[HourlyAggResult]
    res.orderBy(res('hour).asc)
  }

  def daily: TypedDataset[DailyAggResult] = {
    val day: TypedDataset[LocalDate] = consumerRecords.deserialized.map { m =>
      KafkaTimestamp(m.timestamp).local(sparkafka.params.zoneId).toLocalDate
    }
    val res = day.groupBy(day.asCol).agg(count(day.asCol)).as[DailyAggResult]
    res.orderBy(res('date).asc)
  }

  def nullValues: TypedDataset[SparKafkaConsumerRecord[K, V]] =
    consumerRecords.filter(consumerRecords('value).isNone)

  def nullKeys: TypedDataset[SparKafkaConsumerRecord[K, V]] =
    consumerRecords.filter(consumerRecords('key).isNone)

  def values: TypedDataset[V] =
    consumerRecords.select(consumerRecords('value)).as[Option[V]].deserialized.flatMap(x => x)

  def keys: TypedDataset[K] =
    consumerRecords.select(consumerRecords('key)).as[Option[K]].deserialized.flatMap(x => x)

  def toProducerRecords: TypedDataset[SparKafkaProducerRecord[K, V]] =
    SparKafka.toProducerRecords(consumerRecords, sparkafka.params.conversionStrategy)

}

final case class SparKafkaSession(params: SparKafkaParams)(implicit val spark: SparkSession) {

  def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSession =
    copy(params = f(params))

  def datasetFromKafka[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[ConsumerRecordDatasetWithSession[K, V]] =
    SparKafka
      .datasetFromKafka(topic, params.timeRange)
      .map(ConsumerRecordDatasetWithSession(this, _))

  def datasetFromDisk[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[ConsumerRecordDatasetWithSession[K, V]] =
    SparKafka
      .datasetFromDisk(topic, params.timeRange, params.rootPath)
      .map(ConsumerRecordDatasetWithSession(this, _))

  def saveToDisk[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[Unit] =
    SparKafka.saveToDisk(topic, params.timeRange, params.rootPath, params.saveMode)

  def replay[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V]): F[Unit] =
    SparKafka.replay(topic, params).map(_ => print(".")).compile.drain
}

@Lenses final case class SparKafkaSettings(
  conf: SparkConf,
  logLevel: String,
  params: SparKafkaParams) {

  def kms(kmsKey: String): SparKafkaSettings =
    updateSparkConf(
      _.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
        .set("spark.hadoop.fs.s3a.server-side-encryption.key", kmsKey))

  def updateSparkConf(f: SparkConf => SparkConf): SparKafkaSettings =
    SparKafkaSettings.conf.set(f(conf))(this)

  def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSettings =
    SparKafkaSettings.params.set(f(params))(this)

  def setLogLevel(logLevel: String): SparKafkaSettings = copy(logLevel = logLevel)

  def sparKafkaSession: SparKafkaSession = {
    val spk = SparkSession.builder().config(conf).getOrCreate()
    spk.sparkContext.setLogLevel(logLevel)
    SparKafkaSession(params)(spk)
  }

  def sessionResource[F[_]: Sync]: Resource[F, SparKafkaSession] =
    Resource.make(Sync[F].delay(sparKafkaSession))(spk => Sync[F].delay(spk.spark.close()))

  def sessionStream[F[_]: Sync]: Stream[F, SparKafkaSession] =
    Stream.resource(sessionResource)
}

object SparKafkaSettings {

  val default: SparKafkaSettings =
    SparKafkaSettings(new SparkConf, "warn", SparKafkaParams.default).updateSparkConf(
      _.set("spark.master", "local[*]")
        .set("spark.ui.enabled", "true")
        .set("spark.debug.maxToStringFields", "1000")
        .set(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .set("spark.hadoop.fs.s3a.connection.maximum", "100")
        .set("spark.network.timeout", "800")
        .set("spark.streaming.kafka.consumer.poll.ms", "180000")
        .set("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .set("spark.streaming.kafka.allowNonConsecutiveOffsets", "true"))
}
