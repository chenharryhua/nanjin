package com.github.chenharryhua.nanjin.spark

import cats.Endo
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.ChunkSize
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import com.github.chenharryhua.nanjin.spark.kafka.{sk, SparKafkaTopic}
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.refineMV
import fs2.Stream
import fs2.kafka.{KafkaProducer, ProducerRecords, ProducerSettings, Serializer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.typelevel.cats.time.instances.zoneid

final class SparKafkaContext[F[_]](val sparkSession: SparkSession, val kafkaContext: KafkaContext[F])
    extends Serializable with zoneid {

  def topic[K, V](topicDef: TopicDef[K, V]): SparKafkaTopic[F, K, V] =
    new SparKafkaTopic[F, K, V](sparkSession, topicDef.in[F](kafkaContext))

  def topic[K, V](kt: KafkaTopic[F, K, V]): SparKafkaTopic[F, K, V] =
    topic[K, V](kt.topicDef)

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicName): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicDef[K, V](topicName))

  def topic[K: SerdeOf, V: SerdeOf](topicName: TopicNameC): SparKafkaTopic[F, K, V] =
    topic[K, V](TopicName(topicName))

  def sstream(topicName: TopicName): Dataset[NJConsumerRecord[Array[Byte], Array[Byte]]] =
    sk.kafkaSStream(topicName, kafkaContext.settings, sparkSession)

  def sstream(topicName: TopicNameC): Dataset[NJConsumerRecord[Array[Byte], Array[Byte]]] =
    sstream(TopicName(topicName))

  /** download a kafka topic and save to given folder
    * @param topicName
    *   the source topic name
    * @param path
    *   the target folder
    * @param dateRange
    *   datetime range
    */
  def dump(
    topicName: TopicName,
    path: NJPath,
    dateRange: NJDateTimeRange = NJDateTimeRange(kafkaContext.settings.zoneId))(implicit
    F: Async[F]): F[Unit] = {
    val grRdd: F[RDD[String]] = for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      builder = new PullGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      range <- kafkaContext.admin(topicName).offsetRangeFor(dateRange)
    } yield sk.kafkaBatchRDD(kafkaContext.settings, sparkSession, range).map(builder.toJacksonString)
    new RddFileHoarder(grRdd).text(path).withSuffix("jackson.json").run
  }

  /** upload data from given folder to a kafka topic
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
    chunkSize: ChunkSize = refineMV(1000),
    config: Endo[ProducerSettings[F, Array[Byte], Array[Byte]]] = identity)(implicit F: Async[F]): F[Long] = {

    val producerSettings: ProducerSettings[F, Array[Byte], Array[Byte]] =
      config(
        ProducerSettings[F, Array[Byte], Array[Byte]](Serializer[F, Array[Byte]], Serializer[F, Array[Byte]])
          .withProperties(kafkaContext.settings.producerSettings.properties))

    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hadoop  = NJHadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      jackson = hadoop.jackson(schemaPair.consumerRecordSchema)
      builder = new PushGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      num <- hadoop.filesIn(path).flatMap { fs =>
        val ss: Stream[F, ProducerRecords[Array[Byte], Array[Byte]]] =
          fs.foldLeft(Stream.empty.covaryAll[F, ProducerRecords[Array[Byte], Array[Byte]]]) { case (s, p) =>
            s.merge(jackson.source(p).map(builder.fromGenericRecord).chunkN(chunkSize.value))
          }
        KafkaProducer.pipe(producerSettings).apply(ss).compile.fold(0L) { case (sum, prs) => sum + prs.size }
      }
    } yield num
  }
}
