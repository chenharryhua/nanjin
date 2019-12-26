package com.github.chenharryhua.nanjin.spark.kafka

import java.time.Clock
import java.util

import cats.effect.{ConcurrentEffect, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.codec.iso._
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.spark._
import frameless.{TypedDataset, TypedEncoder}
import fs2.{Chunk, Stream}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategy}

import scala.collection.JavaConverters._

private[kafka] object SparKafka {

  private def props(maps: Map[String, String]): util.Map[String, Object] =
    (Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName) ++
      remove(ConsumerConfig.CLIENT_ID_CONFIG)(maps)).mapValues[Object](identity).asJava

  private def path[F[_]](root: StorageRootPath, topic: KafkaTopic[F, _, _]): String =
    root.value + topic.topicDef.topicName

  def datasetFromKafka[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy)(
    implicit sparkSession: SparkSession): F[TypedDataset[NJConsumerRecord[K, V]]] =
    Sync[F].suspend {
      topic.consumer
        .offsetRangeFor(timeRange)
        .map { gtp =>
          KafkaUtils
            .createRDD[Array[Byte], Array[Byte]](
              sparkSession.sparkContext,
              props(topic.context.settings.consumerSettings.config),
              KafkaOffsets.offsetRange(gtp),
              locationStrategy)
            .mapPartitions(_.map(cr => NJConsumerRecord(topic.decoder(cr).optionalDecodeKeyValue)))
        }
        .map(TypedDataset.create(_))
    }

  def datasetFromDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: NJDateTimeRange,
    rootPath: StorageRootPath)(
    implicit sparkSession: SparkSession): F[TypedDataset[NJConsumerRecord[K, V]]] =
    Sync[F].delay {
      val tds =
        TypedDataset.createUnsafe[NJConsumerRecord[K, V]](
          sparkSession.read.parquet(path(rootPath, topic)))
      val inBetween = tds.makeUDF[Long, Boolean](timeRange.isInBetween)
      tds.filter(inBetween(tds('timestamp)))
    }

  def saveToDisk[F[_]: Sync, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
    timeRange: NJDateTimeRange,
    rootPath: StorageRootPath,
    saveMode: SaveMode,
    locationStrategy: LocationStrategy)(implicit sparkSession: SparkSession): F[Unit] =
    datasetFromKafka(topic, timeRange, locationStrategy).map(
      _.write.mode(saveMode).parquet(path(rootPath, topic)))

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

  def uploadToKafka[F[_]: ConcurrentEffect: Timer, K, V](
    topic: => KafkaTopic[F, K, V],
    tds: TypedDataset[NJProducerRecord[K, V]],
    uploadRate: UploadRate
  ): Stream[F, Chunk[RecordMetadata]] =
    tds
      .stream[F]
      .chunkN(uploadRate.batchSize)
      .zipLeft(Stream.fixedRate(uploadRate.duration))
      .evalMap(chk =>
        topic.producer.send(
          chk.mapFilter(Option(_).map(_.withTopic(topic.topicDef.topicName).toProducerRecord))))

  // load data from disk and then upload into kafka
  def replay[F[_]: ConcurrentEffect: Timer, K: TypedEncoder, V: TypedEncoder](
    topic: => KafkaTopic[F, K, V],
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

  def sparkStream[F[_], K: TypedEncoder, V: TypedEncoder](topic: => KafkaTopic[F, K, V])(
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
          .options(toSparkOptions(topic.context.settings.consumerSettings.config))
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
