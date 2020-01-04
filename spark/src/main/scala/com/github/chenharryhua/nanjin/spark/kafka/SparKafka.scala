package com.github.chenharryhua.nanjin.spark.kafka

import java.time.Clock
import java.util

import cats.effect.{Concurrent, ConcurrentEffect, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.{NJRate, NJRootPath}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.api.KafkaConsumerApi
import com.github.chenharryhua.nanjin.kafka.{
  KafkaTopicDescription,
  NJConsumerRecord,
  NJProducerRecord
}
import com.github.chenharryhua.nanjin.spark._
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import fs2.kafka._
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategy}

import scala.collection.JavaConverters._
import cats.effect.ContextShift
import com.github.chenharryhua.nanjin.kafka.codec.iso

private[kafka] object SparKafka {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps) ++ Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName))
      .mapValues[Object](identity)
      .asJava

  private def path(root: NJRootPath, topic: KafkaTopicDescription[_, _]): String =
    root + topic.topicDef.topicName

  private def kafkaRDD[F[_]: Concurrent, K, V](
    topic: KafkaTopicDescription[K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy)(
    implicit sparkSession: SparkSession): F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    Sync[F].suspend {
      KafkaConsumerApi[F, K, V](topic).offsetRangeFor(timeRange).map { gtp =>
        KafkaUtils.createRDD[Array[Byte], Array[Byte]](
          sparkSession.sparkContext,
          props(topic.settings.consumerSettings.config),
          KafkaOffsets.offsetRange(gtp),
          locationStrategy)
      }
    }

  def datasetFromKafka[F[_]: Concurrent, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopicDescription[K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy)(
    implicit sparkSession: SparkSession): F[TypedDataset[NJConsumerRecord[K, V]]] =
    kafkaRDD[F, K, V](topic, timeRange, locationStrategy).map(rdd =>
      TypedDataset.create(rdd.mapPartitions(_.map(cr => topic.decoder(cr).record))))

  def jsonDatasetFromKafka[F[_]: Concurrent, K, V](
    topic: KafkaTopicDescription[K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy)(
    implicit sparkSession: SparkSession): F[TypedDataset[String]] =
    kafkaRDD[F, K, V](topic, timeRange, locationStrategy).map(rdd =>
      TypedDataset.create(rdd.mapPartitions(_.map(cr => topic.toJson(cr).noSpaces))))

  def datasetFromDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopicDescription[K, V],
    timeRange: NJDateTimeRange,
    rootPath: NJRootPath)(
    implicit sparkSession: SparkSession): F[TypedDataset[NJConsumerRecord[K, V]]] =
    Sync[F].delay {
      val tds =
        TypedDataset.createUnsafe[NJConsumerRecord[K, V]](
          sparkSession.read.parquet(path(rootPath, topic)))
      val inBetween = tds.makeUDF[Long, Boolean](timeRange.isInBetween)
      tds.filter(inBetween(tds('timestamp)))
    }

  def saveToDisk[F[_]: Concurrent, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopicDescription[K, V],
    timeRange: NJDateTimeRange,
    rootPath: NJRootPath,
    saveMode: SaveMode,
    locationStrategy: LocationStrategy)(implicit sparkSession: SparkSession): F[Unit] =
    datasetFromKafka[F, K, V](topic, timeRange, locationStrategy)
      .map(_.write.mode(saveMode).parquet(path(rootPath, topic)))

  def toProducerRecords[F[_], K: TypedEncoder, V: TypedEncoder](
    tds: TypedDataset[NJConsumerRecord[K, V]],
    ct: ConversionTactics,
    clock: Clock): TypedDataset[NJProducerRecord[K, V]] = {
    val sorted = tds.orderBy(tds('timestamp).asc, tds('offset).asc)
    ct match {
      case ConversionTactics(true, true) =>
        sorted.deserialized.map(_.toNJProducerRecord)
      case ConversionTactics(false, true) =>
        sorted.deserialized.map(_.toNJProducerRecord.withoutPartition)
      case ConversionTactics(true, false) =>
        sorted.deserialized.map(_.toNJProducerRecord.withNow(clock))
      case ConversionTactics(false, false) =>
        sorted.deserialized.map(_.toNJProducerRecord.withNow(clock).withoutPartition)
    }
  }

  // upload to kafka
  def uploadToKafka[F[_]: ConcurrentEffect: ContextShift: Timer, K, V](
    topic: KafkaTopicDescription[K, V],
    tds: TypedDataset[NJProducerRecord[K, V]],
    uploadRate: NJRate
  ): Stream[F, Chunk[RecordMetadata]] =
    for {
      prd <- producerStream[F].using(topic.fs2ProducerSettings[F])
      fpr <- tds
        .stream[F]
        .chunkN(uploadRate.batchSize)
        .zipLeft(Stream.fixedRate(uploadRate.duration))
        .evalMap { chk =>
          prd.produce(ProducerRecords[Chunk, K, V](chk.map(d =>
            iso.isoFs2ProducerRecord[K, V].reverseGet(d.toProducerRecord))))
        }
      rst <- Stream.eval(fpr)
    } yield rst.records.map(_._2)

  // load data from disk and then upload into kafka
  def replay[F[_]: ConcurrentEffect: ContextShift: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: KafkaTopicDescription[K, V],
    params: SparKafkaParams)(
    implicit sparkSession: SparkSession): Stream[F, Chunk[RecordMetadata]] =
    for {
      ds <- Stream.eval(
        datasetFromDisk[F, K, V](topic, params.timeRange, params.rootPath)
          .map(_.repartition(params.repartition)))
      res <- uploadToKafka(
        topic,
        toProducerRecords(ds, params.conversionTactics, params.clock),
        params.uploadRate)
    } yield res

  def sparkStream[F[_], K: TypedEncoder, V: TypedEncoder](topic: KafkaTopicDescription[K, V])(
    implicit spark: SparkSession): TypedDataset[NJConsumerRecord[K, V]] = {
    def toSparkOptions(m: Map[String, String]): Map[String, String] = {
      val rm1 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])
      val rm2 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
      rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
    }
    import spark.implicits._

    TypedDataset
      .create(
        spark.readStream
          .format("kafka")
          .options(toSparkOptions(topic.settings.consumerSettings.config))
          .option("subscribe", topic.topicDef.topicName)
          .load()
          .as[NJConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .mapPartitions { msgs =>
        val decoder = (msg: NJConsumerRecord[Array[Byte], Array[Byte]]) =>
          NJConsumerRecord[K, V](
            msg.partition,
            msg.offset,
            msg.timestamp,
            msg.key.flatMap(k   => topic.codec.keyCodec.tryDecode(k).toOption),
            msg.value.flatMap(v => topic.codec.valueCodec.tryDecode(v).toOption),
            msg.topic,
            msg.timestampType
          )
        msgs.map(decoder)
      }
  }
}
