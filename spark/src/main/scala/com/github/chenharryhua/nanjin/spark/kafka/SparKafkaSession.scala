package com.github.chenharryhua.nanjin.spark.kafka

import java.util

import cats.effect.{ConcurrentEffect, ContextShift, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.common.{NJRootPath, UpdateParams}
import com.github.chenharryhua.nanjin.kafka.api.KafkaConsumerApi
import com.github.chenharryhua.nanjin.kafka.codec.iso
import com.github.chenharryhua.nanjin.kafka.{
  GenericTopicPartition,
  KafkaOffsetRange,
  KafkaTopicDescription,
  NJConsumerRecord,
  NJProducerRecord
}
import com.github.chenharryhua.nanjin.spark._
import frameless.{TypedDataset, TypedEncoder}
import fs2.kafka._
import fs2.{Chunk, Stream}
import monocle.function.At.remove
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}

import scala.collection.JavaConverters._

final class SparKafkaSession[K, V](
  description: KafkaTopicDescription[K, V],
  params: SparKafkaParams)(implicit val sparkSession: SparkSession)
    extends UpdateParams[SparKafkaParams, SparKafkaSession[K, V]] with Serializable {

  override def updateParams(f: SparKafkaParams => SparKafkaParams): SparKafkaSession[K, V] =
    new SparKafkaSession[K, V](description, f(params))

  private def props: util.Map[String, Object] =
    (remove(ConsumerConfig.CLIENT_ID_CONFIG)(description.settings.consumerSettings.config) ++ Map(
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName))
      .mapValues[Object](identity)
      .asJava

  private def offsetRange(range: GenericTopicPartition[KafkaOffsetRange]): Array[OffsetRange] =
    range.value.toArray.map {
      case (tp, r) => OffsetRange.create(tp, r.from.value, r.until.value)
    }

  private val path: String = description.settings.rootPath + description.topicDef.topicName

  private def kafkaRDD[F[_]: Sync]: F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    KafkaConsumerApi(description).use(_.offsetRangeFor(params.timeRange)).map { gtp =>
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        sparkSession.sparkContext,
        props,
        offsetRange(gtp),
        params.locationStrategy)
    }

  def datasetFromKafka[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[TypedDataset[NJConsumerRecord[K, V]]] =
    kafkaRDD.map(rdd =>
      TypedDataset.create(rdd.mapPartitions(_.map(cr => description.decoder(cr).record))))

  def datasetFromKafka[F[_]: Sync, K1: TypedEncoder, V1: TypedEncoder](
    transKey: K => K1,
    transVal: V => V1): F[TypedDataset[NJConsumerRecord[K1, V1]]] =
    kafkaRDD.map(rdd =>
      TypedDataset.create(
        rdd.mapPartitions(_.map(cr => description.decoder(cr).record.bimap(transKey, transVal)))))

  def jsonDatasetFromKafka[F[_]: Sync]: F[TypedDataset[String]] =
    kafkaRDD.map(rdd =>
      TypedDataset.create(rdd.mapPartitions(_.map(cr => description.toJson(cr).noSpaces))))

  def datasetFromDisk(
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): TypedDataset[NJConsumerRecord[K, V]] = {
    val tds =
      TypedDataset.createUnsafe[NJConsumerRecord[K, V]](sparkSession.read.parquet(path))
    val inBetween = tds.makeUDF[Long, Boolean](params.timeRange.isInBetween)
    tds.filter(inBetween(tds('timestamp)))
  }

  def saveToDisk[F[_]: Sync](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] =
    datasetFromKafka.map(_.write.mode(params.saveMode).parquet(path))

  // upload to kafka
  def uploadToKafka[F[_]: ConcurrentEffect: Timer: ContextShift](
    tds: TypedDataset[NJProducerRecord[K, V]]): Stream[F, ProducerResult[K, V, Unit]] =
    tds
      .stream[F]
      .chunkN(params.uploadRate.batchSize)
      .zipLeft(Stream.fixedRate(params.uploadRate.duration))
      .map(chk =>
        ProducerRecords[Chunk, K, V](
          chk.map(d => iso.isoFs2ProducerRecord[K, V].reverseGet(d.toProducerRecord))))
      .through(produce(description.fs2ProducerSettings[F]))

  // load data from disk and then upload into kafka
  def replay[F[_]: ConcurrentEffect: Timer: ContextShift](
    implicit
    keyEncoder: TypedEncoder[K],
    valEncoder: TypedEncoder[V]): F[Unit] = {
    val run = for {
      ds <- Stream(datasetFromDisk.repartition(params.repartition))
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
          .options(toSparkOptions(description.settings.consumerSettings.config))
          .option("subscribe", description.topicDef.topicName)
          .load()
          .as[NJConsumerRecord[Array[Byte], Array[Byte]]])
      .deserialized
      .mapPartitions { msgs =>
        val decoder = (msg: NJConsumerRecord[Array[Byte], Array[Byte]]) =>
          NJConsumerRecord[K, V](
            msg.partition,
            msg.offset,
            msg.timestamp,
            msg.key.flatMap(k   => description.codec.keyCodec.tryDecode(k).toOption),
            msg.value.flatMap(v => description.codec.valueCodec.tryDecode(v).toOption),
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
    datasetFromKafka.map(ds => new Statistics(params.zoneId, ds.dataset))
}
