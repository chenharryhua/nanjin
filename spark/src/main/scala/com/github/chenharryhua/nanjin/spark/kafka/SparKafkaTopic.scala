package com.github.chenharryhua.nanjin.spark.kafka

import cats.Foldable
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, utils}
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

  private def downloadKafka(dateTimeRange: NJDateTimeRange)(implicit F: Async[F]): F[CrRdd[K, V]] =
    sk.kafkaBatch(topic, sparkSession, dateTimeRange).map(crRdd)

  /** download topic according to datetime
    *
    * @param dtr
    *   : datetime
    */
  def fromKafka(dtr: NJDateTimeRange)(implicit F: Async[F]): F[CrRdd[K, V]] =
    downloadKafka(dtr)

  /** download all topic data, up to now
    */
  def fromKafka(implicit F: Async[F]): F[CrRdd[K, V]] =
    fromKafka(NJDateTimeRange(utils.sparkZoneId(sparkSession)))

  /** download topic according to offset range
    * @param offsets
    *
    * partition -> (start-offset(inclusive), end-offset(exclusive))
    *
    * @return
    *   CrRdd
    */
  def fromKafka(offsets: Map[Int, (Long, Long)])(implicit F: Async[F]): F[CrRdd[K, V]] =
    KafkaContext[F](topic.settings)
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
      .map(offsetRange => crRdd(sk.kafkaBatch(topic, sparkSession, offsetRange)))

  /** load topic data from disk
    */

  def load: LoadTopicFile[F, K, V] = new LoadTopicFile[F, K, V](topic, sparkSession)

  /** rdd and dataset
    */

  def crRdd(rdd: RDD[NJConsumerRecord[K, V]]): CrRdd[K, V] =
    new CrRdd[K, V](rdd, avroKeyCodec, avroValCodec, sparkSession)

  def emptyCrRdd: CrRdd[K, V] =
    crRdd(sparkSession.sparkContext.emptyRDD[NJConsumerRecord[K, V]])

  def prRdd(rdd: RDD[NJProducerRecord[K, V]]): PrRdd[K, V] =
    new PrRdd[K, V](rdd, topic.topicDef.producerCodec)

  def prRdd[G[_]: Foldable](list: G[NJProducerRecord[K, V]]): PrRdd[K, V] =
    prRdd(sparkSession.sparkContext.parallelize(list.toList))

  def emptyPrRdd: PrRdd[K, V] =
    prRdd(sparkSession.sparkContext.emptyRDD[NJProducerRecord[K, V]])

}
