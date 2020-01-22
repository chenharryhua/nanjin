package com.github.chenharryhua.nanjin.spark.kafka

import java.util

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.{NJFileFormat, UpdateParams}
import com.github.chenharryhua.nanjin.kafka.api.KafkaConsumerApi
import com.github.chenharryhua.nanjin.kafka.codec.iso
import com.github.chenharryhua.nanjin.kafka.{
  KafkaOffsetRange,
  KafkaTopicDescription,
  KafkaTopicPartition,
  NJConsumerRecord,
  NJProducerRecord
}
import com.github.chenharryhua.nanjin.spark._
import frameless.{TypedDataset, TypedEncoder}
import fs2.kafka._
import fs2.{Chunk, Stream}
import io.circe.syntax._
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.log4s.Logger

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

final class SparKafkaSession[K, V](kafkaDesc: KafkaTopicDescription[K, V], params: SparKafkaParams)(
  implicit val sparkSession: SparkSession)
    extends UpdateParams[SparKafkaParams, SparKafkaSession[K, V]] with Serializable {
  private[this] val logger: Logger = org.log4s.getLogger

  override def withParamUpdate(f: SparKafkaParams => SparKafkaParams): SparKafkaSession[K, V] =
    new SparKafkaSession[K, V](kafkaDesc, f(params))

  private def props: util.Map[String, Object] =
    (remove(ConsumerConfig.CLIENT_ID_CONFIG)(kafkaDesc.settings.consumerSettings.config) ++ Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName))
      .mapValues[Object](identity)
      .asJava

  private def offsetRanges(
    range: KafkaTopicPartition[Option[KafkaOffsetRange]]): Array[OffsetRange] =
    range.flatten[KafkaOffsetRange].value.toArray.map {
      case (tp, r) => OffsetRange.create(tp, r.from.value, r.until.value)
    }

  private def kafkaRDD[F[_]: Sync]: F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    KafkaConsumerApi(kafkaDesc).use(_.offsetRangeFor(params.timeRange)).map { gtp =>
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        sparkSession.sparkContext,
        props,
        offsetRanges(gtp),
        params.locationStrategy)
    }

  def datasetFromKafka[F[_]: Sync, A](f: NJConsumerRecord[K, V] => A)(
    implicit ev: TypedEncoder[A]): F[TypedDataset[A]] = {
    implicit val tag: ClassTag[A] = ev.classTag
    kafkaRDD.map { rdd =>
      TypedDataset.create(rdd.mapPartitions(_.map { m =>
        val r = kafkaDesc.decoder(m).logRecord.run
        r._1.map(x => logger.warn(x.error)(x.metaInfo))
        f(r._2)
      }))
    }
  }

  def datasetFromKafka[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[TypedDataset[NJConsumerRecord[K, V]]] =
    datasetFromKafka[F, NJConsumerRecord[K, V]](identity)

  def load(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[NJConsumerRecord[K, V]] = {
    val path = params.pathBuilder(kafkaDesc.topicName)
    val tds = params.fileFormat match {
      case NJFileFormat.Parquet | NJFileFormat.Avro =>
        TypedDataset.createUnsafe[NJConsumerRecord[K, V]](
          sparkSession.read.format(params.fileFormat.format).load(path))
      case NJFileFormat.Json =>
        TypedDataset
          .create(sparkSession.read.textFile(path))
          .deserialized
          .flatMap(kafkaDesc.fromJsonStr(_).toOption)
    }
    val inBetween = tds.makeUDF[Long, Boolean](params.timeRange.isInBetween)
    tds.filter(inBetween(tds('timestamp)))
  }

  def save[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    params.fileFormat match {
      case NJFileFormat.Parquet | NJFileFormat.Avro =>
        datasetFromKafka.map(
          _.write
            .mode(params.saveMode)
            .format(params.fileFormat.format)
            .save(params.pathBuilder(kafkaDesc.topicName)))
      case NJFileFormat.Json => saveJson
    }

  def saveJson[F[_]: Sync]: F[Unit] = {
    import kafkaDesc.topicDef.{jsonKeyEncoder, jsonValueEncoder}
    datasetFromKafka[F, String](_.asJson.noSpaces)
      .map(_.write.mode(params.saveMode).text(params.pathBuilder(kafkaDesc.topicName)))
  }

  // upload to kafka
  def uploadToKafka[F[_]: ConcurrentEffect: Timer: ContextShift](
    tds: TypedDataset[NJProducerRecord[K, V]]): Stream[F, ProducerResult[K, V, Unit]] =
    tds
      .stream[F]
      .chunkN(params.uploadRate.batchSize)
      .metered(params.uploadRate.duration)
      .map(chk =>
        ProducerRecords[Chunk, K, V](
          chk.map(d => iso.isoFs2ProducerRecord[K, V].reverseGet(d.toProducerRecord))))
      .through(produce(kafkaDesc.fs2ProducerSettings[F]))

  // load data from disk and then upload into kafka
  def replay[F[_]: ConcurrentEffect: Timer: ContextShift](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] = {
    val run = for {
      ds <- Stream(load.repartition(params.repartition))
      res <- uploadToKafka(ds.toProducerRecords(params.conversionTactics, params.clock))
    } yield res
    run.map(_ => print(".")).compile.drain
  }

  def sparkStream(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[NJConsumerRecord[K, V]] = {
    def toSparkOptions(m: Map[String, String]): Map[String, String] = {
      val rm1 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])
      val rm2 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
      rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
    }
    import sparkSession.implicits._

    TypedDataset
      .create(
        sparkSession.readStream
          .format("kafka")
          .options(toSparkOptions(kafkaDesc.settings.consumerSettings.config))
          .option("subscribe", kafkaDesc.topicDef.topicName.value)
          .load()
          .as[NJConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .mapPartitions { msgs =>
        val decoder = (msg: NJConsumerRecord[Array[Byte], Array[Byte]]) =>
          NJConsumerRecord[K, V](
            msg.partition,
            msg.offset,
            msg.timestamp,
            msg.key.flatMap(k   => kafkaDesc.codec.keyCodec.tryDecode(k).toOption),
            msg.value.flatMap(v => kafkaDesc.codec.valueCodec.tryDecode(v).toOption),
            msg.topic,
            msg.timestampType
          )
        msgs.map(decoder)
      }
  }

  def stats[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Statistics[K, V]] =
    datasetFromKafka.map(ds => new Statistics(params.timeRange.zoneId, ds.dataset))
}
