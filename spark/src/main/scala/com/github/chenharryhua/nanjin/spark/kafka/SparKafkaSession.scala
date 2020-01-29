package com.github.chenharryhua.nanjin.spark.kafka

import java.util

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.{NJFileFormat, UpdateParams}
import com.github.chenharryhua.nanjin.kafka.common.{
  KafkaOffsetRange,
  KafkaTopicPartition,
  NJConsumerRecord,
  NJProducerRecord
}
import com.github.chenharryhua.nanjin.kafka.{KafkaConsumerApi, KafkaTopicDescription}
import frameless.{TypedDataset, TypedEncoder, TypedExpressionEncoder}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.log4s.Logger

import scala.collection.JavaConverters._

trait FsmSparKafka extends Serializable

final class SparKafkaSession[K, V](
  val topicDesc: KafkaTopicDescription[K, V],
  val params: SparKafkaParams)(implicit sparkSession: SparkSession)
    extends FsmSparKafka with UpdateParams[SparKafkaParams, SparKafkaSession[K, V]] {
  private val logger: Logger = org.log4s.getLogger

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

  private def kafkaRDD[F[_]: Sync]: F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    KafkaConsumerApi(topicDesc).use(_.offsetRangeFor(params.timeRange)).map { gtp =>
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        sparkSession.sparkContext,
        props(topicDesc.settings.consumerSettings.config),
        offsetRanges(gtp),
        params.locationStrategy)
    }

  override def withParamUpdate(f: SparKafkaParams => SparKafkaParams): SparKafkaSession[K, V] =
    new SparKafkaSession[K, V](topicDesc, f(params))

  def fromKafka[F[_]: Sync, A](f: NJConsumerRecord[K, V] => A)(
    implicit ev: TypedEncoder[A]): F[TypedDataset[A]] = {
    import ev.classTag
    kafkaRDD[F]
      .map(_.mapPartitions(_.map { m =>
        val r = topicDesc.decoder(m).logRecord.run
        r._1.map(x => logger.warn(x.error)(x.metaInfo))
        f(r._2)
      }))
      .map(rdd => TypedDataset.create(rdd))
  }

  def fromKafka[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[FsmConsumerRecords[F, K, V]] =
    fromKafka(identity).map(crDataset)

  def fromDisk[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] = {
    val path   = params.getPath(topicDesc.topicName)
    val schema = TypedExpressionEncoder.targetStructType(TypedEncoder[NJConsumerRecord[K, V]])
    val tds: TypedDataset[NJConsumerRecord[K, V]] = {
      params.fileFormat match {
        case NJFileFormat.Avro | NJFileFormat.Parquet | NJFileFormat.Json =>
          TypedDataset.createUnsafe[NJConsumerRecord[K, V]](
            sparkSession.read.schema(schema).format(params.fileFormat.format).load(path))
        case NJFileFormat.Jackson =>
          TypedDataset
            .create(sparkSession.read.textFile(path))
            .deserialized
            .flatMap(m => topicDesc.fromJackson(m).toOption)
      }
    }
    val inBetween = tds.makeUDF[Long, Boolean](params.timeRange.isInBetween)
    crDataset(tds.filter(inBetween(tds('timestamp))))
  }

  def crDataset[F[_], A](cr: TypedDataset[NJConsumerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmConsumerRecords[F, K, V] =
    new FsmConsumerRecords[F, K, V](cr.dataset, this)

  def prDataset[F[_], A](pr: TypedDataset[NJProducerRecord[K, V]])(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmProducerRecords[F, K, V] =
    new FsmProducerRecords[F, K, V](pr.dataset, this)

  def replay[F[_]: ConcurrentEffect: Timer: ContextShift](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    fromDisk.toProducerRecords.upload.map(_ => print(".")).compile.drain

  def sparkStreaming[F[_]](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): FsmSparkStreaming[F, NJConsumerRecord[K, V]] = {
    def toSparkOptions(m: Map[String, String]): Map[String, String] = {
      val rm1 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])
      val rm2 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
      rm1.andThen(rm2)(m).map { case (k, v) => s"kafka.$k" -> v }
    }
    import sparkSession.implicits._

    val tds = TypedDataset
      .create(
        sparkSession.readStream
          .format("kafka")
          .options(toSparkOptions(topicDesc.settings.consumerSettings.config))
          .option("subscribe", topicDesc.topicDef.topicName.value)
          .load()
          .as[NJConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .mapPartitions { msgs =>
        val decoder = (msg: NJConsumerRecord[Array[Byte], Array[Byte]]) =>
          NJConsumerRecord[K, V](
            msg.partition,
            msg.offset,
            msg.timestamp,
            msg.key.flatMap(k   => topicDesc.codec.keyCodec.tryDecode(k).toOption),
            msg.value.flatMap(v => topicDesc.codec.valueCodec.tryDecode(v).toOption),
            msg.topic,
            msg.timestampType
          )
        msgs.map(decoder)
      }
    new FsmSparkStreaming[F, NJConsumerRecord[K, V]](tds.dataset, params)
  }
}
