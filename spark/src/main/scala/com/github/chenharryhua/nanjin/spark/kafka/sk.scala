package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.datetime.{NJDateTimeRange, NJTimestamp}
import com.github.chenharryhua.nanjin.kafka.{KafkaOffsetRange, KafkaTopic, KafkaTopicPartition}
import com.github.chenharryhua.nanjin.spark.{AvroTypedEncoder, SparkDatetimeConversionConstant}
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
import scala.collection.JavaConverters.*

private[kafka] object sk {

  // https://spark.apache.org/docs/3.0.1/streaming-kafka-0-10-integration.html
  private def props(config: Map[String, String]): util.Map[String, Object] =
    (config ++ // override deserializers if any
      Map(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName
      )).mapValues[Object](identity).asJava

  private def offsetRanges(range: KafkaTopicPartition[Option[KafkaOffsetRange]]): Array[OffsetRange] =
    range.flatten.value.toArray.map { case (tp, r) =>
      OffsetRange.create(tp, r.from.value, r.until.value)
    }

  private def kafkaRDD[F[_]: Sync, K, V](
    topic: KafkaTopic[F, K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy,
    sparkSession: SparkSession): F[RDD[ConsumerRecord[Array[Byte], Array[Byte]]]] =
    topic.shortLiveConsumer.use(_.offsetRangeFor(timeRange)).map { gtp =>
      KafkaUtils.createRDD[Array[Byte], Array[Byte]](
        sparkSession.sparkContext,
        props(topic.context.settings.consumerSettings.config),
        offsetRanges(gtp),
        locationStrategy)
    }

  def kafkaDStream[F[_]: Sync, K, V](
    topic: KafkaTopic[F, K, V],
    streamingContext: StreamingContext,
    locationStrategy: LocationStrategy): F[DStream[NJConsumerRecord[K, V]]] =
    topic.shortLiveConsumer.use(_.partitionsFor).map { topicPartitions =>
      val consumerStrategy: ConsumerStrategy[Array[Byte], Array[Byte]] =
        ConsumerStrategies.Assign[Array[Byte], Array[Byte]](
          topicPartitions.value,
          props(topic.context.settings.consumerSettings.config).asScala)
      KafkaUtils.createDirectStream(streamingContext, locationStrategy, consumerStrategy).mapPartitions { ms =>
        ms.map(m => NJConsumerRecordWithError(topic.codec, m).toNJConsumerRecord)
      }
    }

  def kafkaBatch[F[_]: Sync, K, V](
    topic: KafkaTopic[F, K, V],
    timeRange: NJDateTimeRange,
    locationStrategy: LocationStrategy,
    sparkSession: SparkSession): F[RDD[NJConsumerRecordWithError[K, V]]] =
    kafkaRDD[F, K, V](topic, timeRange, locationStrategy, sparkSession).map(_.mapPartitions { ms =>
      ms.map(m => NJConsumerRecordWithError(topic.codec, m))
    })

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

  def kafkaSStream[F[_], K, V, A](topic: KafkaTopic[F, K, V], ate: AvroTypedEncoder[A], sparkSession: SparkSession)(
    f: NJConsumerRecord[K, V] => A): Dataset[A] = {
    import sparkSession.implicits.*
    sparkSession.readStream
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
      }(ate.sparkEncoder)
  }
}
