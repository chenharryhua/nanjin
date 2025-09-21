package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.*
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJHeader}
import monocle.function.At.{atMap, remove}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange as SOffsetRange}

import java.sql.Timestamp
import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.*

private[spark] object sk {

  // https://spark.apache.org/docs/3.0.1/streaming-kafka-0-10-integration.html
  private def props(config: Map[String, String]): util.Map[String, Object] =
    (config.updatedWith(ConsumerConfig.GROUP_ID_CONFIG) {
      case gid @ Some(_) => gid
      case None          => Some(UUID.randomUUID().show)
    } ++ // override deserializers if any
      Map(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName
      )).view.mapValues[Object](identity).toMap.asJava

  private def offsetRanges(range: TopicPartitionMap[Option[OffsetRange]]): Array[SOffsetRange] =
    range.flatten.value.toArray.map { case (tp, r) =>
      SOffsetRange.create(tp, r.from, r.until)
    }

  private def kafkaBatchRDD(
    consumerSettings: KafkaConsumerSettings,
    ss: SparkSession,
    offsetRange: TopicPartitionMap[Option[OffsetRange]]): RDD[ConsumerRecord[Array[Byte], Array[Byte]]] =
    KafkaUtils.createRDD[Array[Byte], Array[Byte]](
      ss.sparkContext,
      props(consumerSettings.properties),
      offsetRanges(offsetRange),
      LocationStrategies.PreferConsistent)

  def kafkaBatch[K, V](
    ss: SparkSession,
    consumerSettings: KafkaConsumerSettings,
    serde: TopicSerde[K, V],
    offsetRange: TopicPartitionMap[Option[OffsetRange]]): RDD[NJConsumerRecord[K, V]] =
    kafkaBatchRDD(consumerSettings, ss, offsetRange).map(serde.toNJConsumerRecord(_))

  def kafkaBatch[F[_]: Async, K, V](
    ss: SparkSession,
    ctx: KafkaContext[F],
    serde: TopicSerde[K, V],
    dateRange: DateTimeRange): F[RDD[NJConsumerRecord[K, V]]] =
    ctx
      .admin(serde.topicName.name)
      .use(_.offsetRangeFor(dateRange))
      .map(kafkaBatch(ss, ctx.settings.consumerSettings, serde, _))

  /** streaming
    */

  //  https://spark.apache.org/docs/3.0.1/structured-streaming-kafka-integration.html
  private def consumerOptions(m: Map[String, String]): Map[String, String] = {
    val rm1 = remove(ConsumerConfig.GROUP_ID_CONFIG)(_: Map[String, String])
    val rm2 = remove(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)(_: Map[String, String])

    val rm3 = remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    val rm4 = remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)(_: Map[String, String])

    val rm5 = remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)(_: Map[String, String])
    val rm6 = remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)(_: Map[String, String])

    val rm7 = remove(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)(_: Map[String, String])
    val rm8 = remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG)(_: Map[String, String])
    rm1.andThen(rm2).andThen(rm3).andThen(rm4).andThen(rm5).andThen(rm6).andThen(rm7).andThen(rm8)(m).map {
      case (k, v) => s"kafka.$k" -> v
    }
  }

  def kafkaSStream(
    topicName: TopicName,
    settings: KafkaSettings,
    ss: SparkSession): Dataset[NJConsumerRecord[Array[Byte], Array[Byte]]] = {
    import ss.implicits.*
    ss.readStream
      .format("kafka")
      .options(consumerOptions(settings.consumerSettings.properties))
      .option("subscribe", topicName.value)
      .option("includeHeaders", "true")
      .load()
      .as[(Array[Byte], Array[Byte], String, Int, Long, Timestamp, Int, Array[NJHeader])]
      .mapPartitions(_.map { case (key, value, topic, partition, offset, timestamp, timestampType, headers) =>
        NJConsumerRecord(
          topic = topic,
          partition = partition,
          offset = offset,
          timestamp = timestamp.getTime,
          timestampType = timestampType,
          serializedKeySize = None,
          serializedValueSize = None,
          key = Option(key),
          value = Option(value),
          headers = Option(headers).traverse(_.toList).flatten,
          leaderEpoch = None
        )
      })
  }
}
