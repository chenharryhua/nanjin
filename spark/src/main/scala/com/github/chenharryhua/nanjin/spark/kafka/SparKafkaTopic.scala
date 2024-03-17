package com.github.chenharryhua.nanjin.spark.kafka

import cats.Foldable
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.{utils, AvroTypedEncoder}
import frameless.TypedEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class SparKafkaTopic[F[_], K, V](val sparkSession: SparkSession, val topic: KafkaTopic[F, K, V])
    extends Serializable {
  override val toString: String = topic.topicName.value

  val topicName: TopicName = topic.topicDef.topicName

  def ate(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): AvroTypedEncoder[NJConsumerRecord[K, V]] =
    AvroTypedEncoder(topic.topicDef)

  private val avroKeyCodec: NJAvroCodec[K] = topic.topicDef.rawSerdes.key.avroCodec
  private val avroValCodec: NJAvroCodec[V] = topic.topicDef.rawSerdes.value.avroCodec

  private def downloadKafka(dateTimeRange: NJDateTimeRange)(implicit F: Async[F]): CrRdd[F, K, V] =
    crRdd(sk.kafkaBatch(topic, sparkSession, dateTimeRange))

  /** download topic according to datetime
    *
    * @param dtr
    *   : datetime
    */
  def fromKafka(dtr: NJDateTimeRange)(implicit F: Async[F]): CrRdd[F, K, V] =
    downloadKafka(dtr)

  /** download all topic data, up to now
    */
  def fromKafka(implicit F: Async[F]): CrRdd[F, K, V] =
    fromKafka(NJDateTimeRange(utils.sparkZoneId(sparkSession)))

  /** download topic according to offset range
    * @param offsets
    *
    * partition -> (start-offset(inclusive), end-offset(exclusive))
    *
    * @return
    *   CrRdd
    */
  def fromKafka(offsets: Map[Int, (Long, Long)])(implicit F: Async[F]): CrRdd[F, K, V] =
    crRdd(
      topic.context
        .admin(topicName)
        .partitionsFor
        .map { partitions =>
          val topicPartition = partitions.value.map { tp =>
            val ofs: Option[KafkaOffsetRange] =
              offsets
                .get(tp.partition())
                .flatMap(se => KafkaOffsetRange(KafkaOffset(se._1), KafkaOffset(se._2)))
            tp -> ofs
          }.toMap
          KafkaTopicPartition(topicPartition)
        }
        .flatMap(offsetRange => F.interruptible(sk.kafkaBatch(topic, sparkSession, offsetRange))))

  /** load topic data from disk
    */

  def load(implicit F: Sync[F]): LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](topic, sparkSession)

  /** rdd and dataset
    */

  def crRdd(rdd: F[RDD[NJConsumerRecord[K, V]]])(implicit F: Sync[F]): CrRdd[F, K, V] =
    new CrRdd[F, K, V](rdd, avroKeyCodec, avroValCodec, sparkSession)

  def emptyCrRdd(implicit F: Sync[F]): CrRdd[F, K, V] =
    crRdd(F.interruptible(sparkSession.sparkContext.emptyRDD[NJConsumerRecord[K, V]]))

  def prRdd(rdd: F[RDD[NJProducerRecord[K, V]]])(implicit F: Sync[F]): PrRdd[F, K, V] =
    new PrRdd[F, K, V](rdd, topic.topicDef.producerCodec)

  def prRdd[G[_]: Foldable](list: G[NJProducerRecord[K, V]])(implicit F: Sync[F]): PrRdd[F, K, V] =
    prRdd(F.interruptible(sparkSession.sparkContext.parallelize(list.toList)))

  def emptyPrRdd(implicit F: Sync[F]): PrRdd[F, K, V] =
    prRdd(F.interruptible(sparkSession.sparkContext.emptyRDD[NJProducerRecord[K, V]]))

}
