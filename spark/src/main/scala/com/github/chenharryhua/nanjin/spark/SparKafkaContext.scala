package com.github.chenharryhua.nanjin.spark

import cats.Endo
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameL}
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{gr2Jackson, SerdeOf}
import com.github.chenharryhua.nanjin.messages.kafka.{CRMetaInfo, NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.kafka.{sk, SparKafkaTopic, Statistics}
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import com.github.chenharryhua.nanjin.terminals.{toHadoopPath, Hadoop}
import eu.timepit.refined.refineMV
import fs2.{Chunk, Stream}
import fs2.kafka.*
import io.lemonlabs.uri.Url
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.typelevel.cats.time.instances.zoneid

import scala.concurrent.duration.DurationInt

final class SparKafkaContext[F[_]](val sparkSession: SparkSession, val kafkaContext: KafkaContext[F])
    extends Serializable with zoneid {

  val hadoop: Hadoop[F] = sparkSession.hadoop[F]

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
  def dump(topicName: TopicName, path: Url, dateRange: DateTimeRange)(implicit F: Async[F]): F[Long] = {
    val grRdd: F[RDD[String]] = for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      builder = new PullGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      range <- kafkaContext.admin(topicName).offsetRangeFor(dateRange)
    } yield sk
      .kafkaBatchRDD(kafkaContext.settings.consumerSettings, sparkSession, range)
      .flatMap(builder.toGenericRecord(_).flatMap(gr2Jackson(_)).toOption)

    grRdd.flatMap(rdd =>
      new RddFileHoarder(rdd).text(path).withSaveMode(_.Overwrite).withSuffix("jackson.json").runWithCount[F])
  }

  def dump(topicName: TopicName, path: Url)(implicit F: Async[F]): F[Long] =
    dump(topicName, path, DateTimeRange(utils.sparkZoneId(sparkSession)))

  def dump(topicName: TopicNameL, path: Url, dateRange: DateTimeRange)(implicit F: Async[F]): F[Long] =
    dump(TopicName(topicName), path, dateRange)

  def dump(topicName: TopicNameL, path: Url)(implicit F: Async[F]): F[Long] =
    dump(TopicName(topicName), path, DateTimeRange(utils.sparkZoneId(sparkSession)))

  def download[K: SerdeOf, V: SerdeOf](topicName: TopicNameL, path: Url, dateRange: DateTimeRange)(implicit
    F: Async[F]): F[Long] =
    topic[K, V](topicName)
      .fromKafka(dateRange)
      .flatMap(_.output.jackson(path).withSaveMode(_.Overwrite).runWithCount[F])

  def download[K: SerdeOf, V: SerdeOf](topicName: TopicNameL, path: Url)(implicit F: Async[F]): F[Long] =
    download[K, V](topicName, path, DateTimeRange(utils.sparkZoneId(sparkSession)))

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
    path: Url,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      partitions <- kafkaContext.admin(topicName).partitionsFor.map(_.value.size)
      hadoop  = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop.filesIn(path).flatMap { fs =>
        val step = Math.ceil(fs.size / (partitions * 1.0)).toInt
        val downloads: Iterator[F[Long]] = fs.sliding(step, step).map { urls =>
          val ss: Stream[F, Chunk[ProducerRecord[Array[Byte], Array[Byte]]]] = urls
            .map(hadoop.source(_).jackson(chunkSize, schemaPair.consumerSchema))
            .reduce(_ ++ _)
            .chunks
            .map(_.map(builder.fromGenericRecord))
          KafkaProducer.pipe(producerSettings).apply(ss).compile.fold(0L) { case (sum, prs) =>
            sum + prs.size
          }
        }
        F.parSequenceN(partitions)(downloads.toList).map(_.sum)
      }
    } yield num
  }

  def upload(
    topicName: TopicNameL,
    path: Url,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] =
    upload(TopicName(topicName), path, chunkSize, config)

  def upload(topicName: TopicNameL, path: Url)(implicit F: Async[F]): F[Long] =
    upload(TopicName(topicName), path, refineMV(1000), identity)

  def crazyUpload(topicName: TopicNameL, path: Url)(implicit F: Async[F]): F[Long] =
    upload(
      TopicName(topicName),
      path,
      refineMV(1000),
      _.withBatchSize(200000).withLinger(10.milli).withAcks(Acks.One))

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
  def sequentialUpload(
    topicName: TopicName,
    path: Url,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hadoop  = Hadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop
        .filesIn(path)
        .flatMap(
          _.traverse(
            hadoop
              .source(_)
              .jackson(chunkSize, schemaPair.consumerSchema)
              .chunks
              .map(_.map(builder.fromGenericRecord))
              .through(KafkaProducer.pipe(producerSettings))
              .compile
              .fold(0L) { case (sum, prs) => sum + prs.size }))
        .map(_.sum)
    } yield num
  }

  def sequentialUpload(
    topicName: TopicNameL,
    path: Url,
    chunkSize: ChunkSize,
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]])(implicit F: Async[F]): F[Long] =
    sequentialUpload(TopicName(topicName), path, chunkSize, config)

  def sequentialUpload(topicName: TopicNameL, path: Url)(implicit F: Async[F]): F[Long] =
    sequentialUpload(TopicName(topicName), path, refineMV(1000), identity)

  object stats {
    import kafka.{encoderCRMetaInfo, typedEncoderCRMetaInfo}

    private val ate: AvroTypedEncoder[CRMetaInfo] = AvroTypedEncoder[CRMetaInfo]

    def avro(path: Url)(implicit F: Sync[F]): F[Statistics] =
      F.blocking {
        new Statistics(
          sparkSession.read
            .format("avro")
            .schema(ate.sparkSchema)
            .load(toHadoopPath(path).toString)
            .as[CRMetaInfo]
        )
      }

    def jackson(path: Url)(implicit F: Sync[F]): F[Statistics] =
      F.blocking {
        new Statistics(
          sparkSession.read.schema(ate.sparkSchema).json(toHadoopPath(path).toString).as[CRMetaInfo]
        )
      }

    def circe(path: Url)(implicit F: Sync[F]): F[Statistics] =
      jackson(path)

    def parquet(path: Url)(implicit F: Sync[F]): F[Statistics] =
      F.blocking {
        new Statistics(
          sparkSession.read.schema(ate.sparkSchema).parquet(toHadoopPath(path).toString).as[CRMetaInfo]
        )
      }
  }
}
