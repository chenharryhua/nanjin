package com.github.chenharryhua.nanjin.spark.kafka

import java.util

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.common.{
  KafkaOffsetRange,
  KafkaTopicPartition,
  NJConsumerRecord,
  NJProducerRecord
}
import com.github.chenharryhua.nanjin.kafka.{KafkaConsumerApi, KafkaTopicKit}
import com.github.chenharryhua.nanjin.spark._
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import fs2.Stream
import fs2.kafka.{produce, ProducerRecords, ProducerResult}
import io.circe.syntax._
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategy, OffsetRange}
import org.log4s.Logger

import scala.collection.JavaConverters._

private[kafka] object sk {

  private def props(config: Map[String, String]): util.Map[String, Object] =
    (remove(ConsumerConfig.CLIENT_ID_CONFIG)(config) ++ Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName))
      .mapValues[Object](identity)
      .asJava

  private def offsetRanges(
    range: KafkaTopicPartition[Option[KafkaOffsetRange]]): Array[OffsetRange] =
    range.flatten[KafkaOffsetRange].value.toArray.map {
      case (tp, r) => OffsetRange.create(tp, r.from.value, r.until.value)
    }

  private def kafkaRDD[F[_]: Sync, K, V](
    kit: KafkaTopicKit[K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy)(
    implicit sparkSession: SparkSession): F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    KafkaConsumerApi(kit).use(_.offsetRangeFor(timeRange)).map { gtp =>
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        sparkSession.sparkContext,
        props(kit.settings.consumerSettings.config),
        offsetRanges(gtp),
        locationStrategy)
    }

  private val logger: Logger = org.log4s.getLogger("spark.kafka")

  def fromKafka[F[_]: Sync, K, V, A](
    kit: KafkaTopicKit[K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy)(f: NJConsumerRecord[K, V] => A)(
    implicit
    sparkSession: SparkSession,
    encoder: TypedEncoder[A]): F[TypedDataset[A]] = {
    import encoder.classTag
    kafkaRDD[F, K, V](kit, timeRange, locationStrategy)
      .map(_.mapPartitions(_.map { m =>
        val (errs, cr) = kit.decoder(m).logRecord.run
        errs.map(x => logger.warn(x.error)(x.metaInfo))
        f(cr)
      }))
      .map(TypedDataset.create[A])
  }

  def fromDisk[K, V](
    kit: KafkaTopicKit[K, V],
    timeRange: NJDateTimeRange,
    fileFormat: NJFileFormat,
    path: String)(
    implicit
    sparkSession: SparkSession,
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[NJConsumerRecord[K, V]] = {
    val schema = TypedExpressionEncoder.targetStructType(TypedEncoder[NJConsumerRecord[K, V]])
    val tds: TypedDataset[NJConsumerRecord[K, V]] = {
      fileFormat match {
        case NJFileFormat.Avro | NJFileFormat.Parquet | NJFileFormat.Json | NJFileFormat.Text =>
          TypedDataset.createUnsafe[NJConsumerRecord[K, V]](
            sparkSession.read.schema(schema).format(fileFormat.format).load(path))
        case NJFileFormat.Jackson =>
          TypedDataset
            .create(sparkSession.read.textFile(path))
            .deserialized
            .flatMap(m => kit.fromJackson(m).toOption)
      }
    }
    val inBetween = tds.makeUDF[Long, Boolean](timeRange.isInBetween)
    tds.filter(inBetween(tds('timestamp)))
  }

  def save[K, V](
    dataset: TypedDataset[NJConsumerRecord[K, V]],
    kit: KafkaTopicKit[K, V],
    fileFormat: NJFileFormat,
    saveMode: SaveMode,
    path: String): Unit =
    fileFormat match {
      case NJFileFormat.Avro | NJFileFormat.Parquet | NJFileFormat.Json | NJFileFormat.Text =>
        dataset.write.mode(saveMode).format(fileFormat.format).save(path)
      case NJFileFormat.Jackson =>
        dataset.deserialized
          .map(m => kit.topicDef.toJackson(m).noSpaces)
          .write
          .mode(saveMode)
          .text(path)
    }

  def upload[F[_], K, V](
    dataset: TypedDataset[NJProducerRecord[K, V]],
    kit: KafkaTopicKit[K, V],
    repartition: NJRepartition,
    uploadRate: NJUploadRate)(
    implicit
    ce: ConcurrentEffect[F],
    timer: Timer[F],
    cs: ContextShift[F]): Stream[F, ProducerResult[K, V, Unit]] =
    dataset
      .repartition(repartition.value)
      .stream[F]
      .chunkN(uploadRate.batchSize)
      .metered(uploadRate.duration)
      .map(chk => ProducerRecords(chk.map(_.toFs2ProducerRecord(kit.topicName))))
      .through(produce(kit.fs2ProducerSettings[F]))

  /**
    * streaming
    */
  private def startingOffsets(range: KafkaTopicPartition[Option[KafkaOffsetRange]]): String = {
    val start: Map[String, Map[String, Long]] = range
      .flatten[KafkaOffsetRange]
      .value
      .map { case (tp, kor) => (tp.topic(), tp.partition(), kor) }
      .foldLeft(Map.empty[String, Map[String, Long]]) {
        case (sum, item) =>
          val rst = Map(item._2.toString -> item._3.from.value)
          sum.get(item._1) match {
            case Some(m) => Map(item._1 -> (m ++ rst))
            case None    => Map(item._1 -> rst)
          }
      }
    start.asJson.noSpaces
  }

  //  https://spark.apache.org/docs/2.4.5/structured-streaming-kafka-integration.html
  private def consumerOptions(m: Map[String, String]): Map[String, String] = {
    val rm1 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
    val rm2 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])
    val rm3 = remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)(_: Map[String, String])
    val rm4 = remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)(_: Map[String, String])
    val rm5 = remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    val rm6 = remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)(_: Map[String, String])

    rm1.andThen(rm2).andThen(rm3).andThen(rm4).andThen(rm5).andThen(rm6)(m).map {
      case (k, v) => s"kafka.$k" -> v
    }
  }

  def streaming[F[_]: Sync, K, V, A](kit: KafkaTopicKit[K, V], timeRange: NJDateTimeRange)(
    f: NJConsumerRecord[K, V] => A)(
    implicit
    sparkSession: SparkSession,
    encoder: TypedEncoder[A]): F[TypedDataset[A]] = {

    import sparkSession.implicits._
    KafkaConsumerApi(kit).use(_.offsetRangeFor(timeRange)).map { gtp =>
      TypedDataset
        .create(
          sparkSession.readStream
            .format("kafka")
            .options(consumerOptions(kit.settings.consumerSettings.config))
            .option("startingOffsets", startingOffsets(gtp))
            .option("subscribe", kit.topicDef.topicName.value)
            .load()
            .as[NJConsumerRecord[Array[Byte], Array[Byte]]])
        .deserialized
        .mapPartitions { msgs =>
          val decoder = (msg: NJConsumerRecord[Array[Byte], Array[Byte]]) => 
            NJConsumerRecord[K, V](
              msg.partition,
              msg.offset,
              msg.timestamp,
              msg.key.flatMap(k =>
                kit.codec.keyCodec.tryDecode(k).toEither.leftMap(logger.warn(_)("key")).toOption),
              msg.value.flatMap(v =>
                kit.codec.valCodec.tryDecode(v).toEither.leftMap(logger.warn(_)("value")).toOption),
              msg.topic,
              msg.timestampType)
          msgs.map(decoder.andThen(f))
        }
    }
  }
}
