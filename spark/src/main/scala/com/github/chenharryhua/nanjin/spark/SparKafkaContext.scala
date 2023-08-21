package com.github.chenharryhua.nanjin.spark

import cats.Parallel
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.{TopicName, TopicNameC}
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaTopic, PullGenericRecord, TopicDef}
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import com.github.chenharryhua.nanjin.spark.kafka.{sk, SparKafkaTopic}
import com.github.chenharryhua.nanjin.spark.persist.RddFileHoarder
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import fs2.kafka.Acks
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.typelevel.cats.time.instances.zoneid

import scala.concurrent.duration.DurationInt

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

  def dump(topicName: TopicName, path: NJPath, dateRange: NJDateTimeRange)(implicit F: Sync[F]): F[Unit] = {
    val grRdd: F[RDD[String]] = for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      builder = new PullGenericRecord(kafkaContext.settings.schemaRegistrySettings, topicName, schemaPair)
      range <- kafkaContext.shortLiveConsumer(topicName).use(_.offsetRangeFor(dateRange))
    } yield sk.kafkaBatchRDD(kafkaContext.settings, sparkSession, range).map(builder.toJacksonString)
    new RddFileHoarder(grRdd).text(path).withSuffix("jackson.json").run
  }

  def dump(topicName: TopicName, path: NJPath)(implicit F: Sync[F]): F[Unit] =
    dump(topicName, path, NJDateTimeRange(kafkaContext.settings.zoneId))

  def upload(topicName: TopicName, path: NJPath)(implicit F: Async[F], P: Parallel[F]): F[Unit] =
    for {
      schemaPair <- kafkaContext.schemaRegistry.fetchAvroSchema(topicName)
      hdp     = NJHadoop[F](sparkSession.sparkContext.hadoopConfiguration)
      jackson = hdp.jackson(schemaPair.consumerRecordSchema)
      sink = kafkaContext
        .sink(topicName)
        .updateConfig(_.withBatchSize(200000).withLinger(10.milliseconds).withAcks(Acks.One))
        .run
      _ <- hdp.filesIn(path).flatMap(_.parTraverse(jackson.source(_).chunks.through(sink).compile.drain))
    } yield ()
}
