package com.github.chenharryhua.nanjin.spark.kafka

import cats.Foldable
import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJProducerRecord}
import com.github.chenharryhua.nanjin.spark.{utils, SchematizedEncoder}
import frameless.TypedEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

final class SparKafkaTopic[F[_], K, V](val sparkSession: SparkSession, val topic: KafkaTopic[F, K, V])
    extends Serializable {
  override val toString: String = topic.topicName.value

  val topicName: TopicName = topic.topicDef.topicName

  def ate(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): SchematizedEncoder[NJConsumerRecord[K, V]] =
    SchematizedEncoder(topic.topicDef)

  private val avroKeyCodec: AvroCodec[K] = topic.topicDef.codecPair.key.avroCodec
  private val avroValCodec: AvroCodec[V] = topic.topicDef.codecPair.value.avroCodec

  private def downloadKafka(dateTimeRange: DateTimeRange)(implicit F: Async[F]): F[CrRdd[K, V]] =
    sk.kafkaBatch(topic, sparkSession, dateTimeRange).map(crRdd)

  /** download topic according to datetime
    *
    * @param dtr
    *   : datetime
    */
  def fromKafka(dtr: DateTimeRange)(implicit F: Async[F]): F[CrRdd[K, V]] =
    downloadKafka(dtr)

  /** download all topic data, up to now
    */
  def fromKafka(implicit F: Async[F]): F[CrRdd[K, V]] =
    fromKafka(DateTimeRange(utils.sparkZoneId(sparkSession)))

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
      .use(_.partitionsFor.map { partitions =>
        val topicPartition = partitions.value.map { tp =>
          val ofs: Option[OffsetRange] =
            offsets.get(tp.partition()).flatMap(se => OffsetRange(Offset(se._1), Offset(se._2)))
          tp -> ofs
        }.toMap
        TopicPartitionMap(topicPartition)
      })
      .map(offsetRange => crRdd(sk.kafkaBatch(topic, sparkSession, offsetRange)))

  /** load topic data from disk
    */

  def load: LoadTopicFile[K, V] = new LoadTopicFile[K, V](topic.topicDef, sparkSession)

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
