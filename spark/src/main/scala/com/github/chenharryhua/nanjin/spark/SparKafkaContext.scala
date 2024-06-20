package com.github.chenharryhua.nanjin.spark

import cats.Endo
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{gr2Jackson, SerdeOf}
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.kafka.{sk, SparKafkaTopic, Statistics}
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.refineMV
import fs2.Stream
import fs2.kafka.*
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.typelevel.cats.time.instances.zoneid

final class SparKafkaContext[F[_]](val sparkSession: SparkSession, val kafkaContext: KafkaContext[F])
    extends Serializable with zoneid {

  def topic[K, V](topicDef: TopicDef[K, V]): SparKafkaTopic[F, K, V] =
    new SparKafkaTopic[F, K, V](sparkSession, kafkaContext.topic(topicDef))

  def topic[K, V](kt: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
    topic[K, V](kt.topicDef)

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicName): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicNameL): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicName(topicName))

  def sstream(topicName: TopicName): Dataset[NJConsumerRecord[Array[Byte], Array[Byte]]] =
    sk.kafkaSStream(topicName, kafkaContext.settings, sparkSession)

  def sstream(topicName: TopicNameL): Dataset[NJConsumerRecord[Array[Byte], Array[Byte]]] =
    sstream(TopicName(topicName))

  /** download a kafka topic and save to given folder
    *
    * @param topicName
    *   the source topic name
    * @param path
    *   the target folder
    * @param dateRange
    *   datetime range
    */
  def dump(topicName: TopicName, path: NJPath, dateRange: NJDateTimeRange)(implicit F: Async[F]): F[Long] = {
    val grRdd: F[RDD[String]] = for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      builder = new PullGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      range <- kafkaContext.admin(topicName).offsetRangeFor(dateRange)
    } yield sk
      .kafkaBatchRDD(kafkaContext.settings.consumerSettings, sparkSession, range)
      .flatMap(builder.toGenericRecord(_).flatMap(gr2Jackson(_)).toOption)

    grRdd.flatMap(rdd =>
      new RddFileHoarder(rdd)
        .text(path)
        .withSaveMode(_.Append)
        .withSuffix("jackson.json")
        .run[F]
        .as(rdd.count()))
  }

  def dump(topicName: TopicNameL, path: NJPath)(implicit F: Async[F]): F[Long] =
    dump(TopicName(topicName), path, NJDateTimeRange(utils.sparkZoneId(sparkSession)))

  def dump(topicName: TopicName, path: NJPath)(implicit F: Async[F]): F[Long] =
    dump(topicName, path, NJDateTimeRange(utils.sparkZoneId(sparkSession)))

  /** upload data from given folder to a kafka topic. files read in parallel
    *
    * @param topicName
    *   target topic name
    * @param path
    *   the source data files folder
    * @param chunkSize
    *   when set to 1, the data will be sent in order
    * @param config
    *   config fs2.kafka.producer. Acks.All for reliable upload
    * @return
    *   number of records uploaded
    *
    * [[https://www.conduktor.io/kafka/kafka-producer-batching/]]
    */

  def upload(
    topicName: TopicName,
    path: NJPath,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hadoop  = NJHadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      jackson = hadoop.jackson(schemaPair.consumerSchema)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop.filesIn(path).flatMap { fs =>
        val ss: Stream[F, ProducerRecords[Array[Byte], Array[Byte]]] =
          fs.foldLeft(Stream.empty.covaryAll[F, ProducerRecords[Array[Byte], Array[Byte]]]) { case (s, p) =>
            s.merge(jackson.source(p, chunkSize).map(_.map(builder.fromGenericRecord)))
          }
        KafkaProducer.pipe(producerSettings).apply(ss).compile.fold(0L) { case (sum, prs) => sum + prs.size }
      }
    } yield num
  }

  def upload(
    topicName: TopicNameL,
    path: NJPath,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] =
    upload(TopicName(topicName), path, chunkSize, config)
  def upload(topicName: TopicNameL, path: NJPath)(implicit F: Async[F]): F[Long] =
    upload(TopicName(topicName), path, refineMV(1000), identity)

  /** sequentially read files in the folder, sorted by modification time, and upload them into kafka
    *
    * @param topicName
    *   target topic name
    * @param path
    *   the source data folder
    * @param chunkSize
    *   when set to 1, the data will be sent in order
    * @param config
    *   config fs2.kafka.producer. Acks.All for reliable upload
    * @return
    *   number of records uploaded
    */
  def uploadInSequence(
    topicName: TopicName,
    path: NJPath,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hadoop  = NJHadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      jackson = hadoop.jackson(schemaPair.consumerSchema)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop
        .filesIn(path)
        .flatMap(
          _.traverse(
            jackson
              .source(_, chunkSize)
              .map(_.map(builder.fromGenericRecord))
              .through(KafkaProducer.pipe(producerSettings))
              .compile
              .fold(0L) { case (sum, prs) => sum + prs.size }))
        .map(_.sum)
    } yield num
  }

  def uploadInSequence(
    topicName: TopicNameL,
    path: NJPath,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] =
    uploadInSequence(TopicName(topicName), path, chunkSize, config)

  def uploadInSequence(topicName: TopicNameL, path: NJPath)(implicit F: Async[F]): F[Long] =
    uploadInSequence(TopicName(topicName), path, refineMV(1000), identity)

  object stats {
    import kafka.{encoderCRMetaInfo, typedEncoderCRMetaInfo}

    private val ate: AvroTypedEncoder[CRMetaInfo] = AvroTypedEncoder[CRMetaInfo]

    def avro(path: NJPath): Statistics =
      new Statistics(
        sparkSession.read.format("avro").schema(ate.sparkSchema).load(path.pathStr).as[CRMetaInfo]
      )

    def jackson(path: NJPath): Statistics =
      new Statistics(
        sparkSession.read.schema(ate.sparkSchema).json(path.pathStr).as[CRMetaInfo]
      )

    def circe(path: NJPath): Statistics =
      jackson(path)

    def parquet(path: NJPath): Statistics =
      new Statistics(
        sparkSession.read.schema(ate.sparkSchema).parquet(path.pathStr).as[CRMetaInfo]
      )
  }
}
