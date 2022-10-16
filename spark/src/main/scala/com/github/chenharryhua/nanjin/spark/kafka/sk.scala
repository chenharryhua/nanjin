package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Sync
import cats.effect.SyncIO
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaOffsetRange, KafkaSettings, KafkaTopic, KafkaTopicPartition}
import com.github.chenharryhua.nanjin.messages.kafka.{NJConsumerRecord, NJConsumerRecordWithError}
import com.github.chenharryhua.nanjin.spark.SparkDatetimeConversionConstant
import frameless.{TypedEncoder, TypedExpressionEncoder}
import monocle.function.At.{atMap, remove}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.*

import java.util
import scala.jdk.CollectionConverters.*

private[kafka] object sk {

  // https://spark.apache.org/docs/3.0.1/streaming-kafka-0-10-integration.html
  private def props(config: Map[String, String]): util.Map[String, Object] =
    (config ++ // override deserializers if any
      Map(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName
      )).view.mapValues[Object](identity).toMap.asJava

  private def offsetRanges(range: KafkaTopicPartition[Option[KafkaOffsetRange]]): Array[OffsetRange] =
    range.flatten.value.toArray.map { case (tp, r) =>
      OffsetRange.create(tp, r.from.value, r.until.value)
    }

  private def kafkaBatchRDD(
    settings: KafkaSettings,
    ss: SparkSession,
    offsetRange: KafkaTopicPartition[Option[KafkaOffsetRange]])
    : RDD[ConsumerRecord[Array[Byte], Array[Byte]]] =
    KafkaUtils.createRDD[Array[Byte], Array[Byte]](
      ss.sparkContext,
      props(settings.consumerSettings.config),
      offsetRanges(offsetRange),
      LocationStrategies.PreferConsistent)

  def kafkaBatch[F[_], K, V](
    topic: KafkaTopic[F, K, V],
    ss: SparkSession,
    offsetRange: KafkaTopicPartition[Option[KafkaOffsetRange]]): RDD[NJConsumerRecordWithError[K, V]] =
    kafkaBatchRDD(topic.context.settings, ss, offsetRange).map(topic.decode(_))

  def kafkaBatch[F[_], K, V](
    topic: KafkaTopic[F, K, V],
    ss: SparkSession,
    dateRange: NJDateTimeRange): RDD[NJConsumerRecordWithError[K, V]] = {
    val offsetRange: KafkaTopicPartition[Option[KafkaOffsetRange]] =
      topic.context.settings
        .context[SyncIO]
        .byteTopic(topic.topicName)
        .shortLiveConsumer
        .use(_.offsetRangeFor(dateRange))
        .unsafeRunSync()
    kafkaBatch(topic, ss, offsetRange)
  }

  def kafkaDStream[F[_]: Sync, K, V](
    topic: KafkaTopic[F, K, V],
    streamingContext: StreamingContext): F[DStream[NJConsumerRecord[K, V]]] =
    topic.shortLiveConsumer.use(_.partitionsFor).map { topicPartitions =>
      val consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]] =
        ConsumerStrategies.Assign[Array[Byte], Array[Byte]](
          topicPartitions.value,
          props(topic.context.settings.consumerSettings.config).asScala)
      KafkaUtils
        .createDirectStream(streamingContext, LocationStrategies.PreferConsistent, consumerStrategy)
        .map(m => topic.decode(m).toNJConsumerRecord)
    }

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

  def kafkaSStream[F[_], K, V, A](topic: KafkaTopic[F, K, V], te: TypedEncoder[A], ss: SparkSession)(
    f: NJConsumerRecord[K, V] => A): Dataset[A] = {
    import ss.implicits.*
    ss.readStream
      .format("kafka")
      .options(consumerOptions(topic.context.settings.consumerSettings.config))
      .option("subscribe", topic.topicName.value)
      .load()
      .as[NJConsumerRecord[Array[Byte], Array[Byte]]]
      .mapPartitions { ms =>
        ms.map { cr =>
          val njcr: NJConsumerRecord[K, V] =
            cr.bimap(topic.codec.keyCodec.tryDecode(_).toOption, topic.codec.valCodec.tryDecode(_).toOption)
              .flatten[K, V]
          f(NJConsumerRecord.timestamp.modify(_ * SparkDatetimeConversionConstant)(njcr))
        }
      }(TypedExpressionEncoder(te))
  }
}
