package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.sksamuel.avro4s
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
trait SparkafkaApi extends Serializable {}

object abc {
  def decoder = SparkMain.topic.valueIso.get _
}

final class SparkafkaApiImpl(spark: SparkSession) extends SparkafkaApi {
  import spark.implicits._

  def valueDataset[F[_], K, V: sql.Encoder: ClassTag](
    topic: KafkaTopic[F, K, V]): Dataset[Payment] = {
    val deser = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      "key.deserializer" -> classOf[ByteArrayDeserializer].getName, // topic.keySerde.deserializer.getClass.getName,
      "value.deserializer" -> classOf[ByteArrayDeserializer].getName //gv.deserializer.getClass.getName // classOf[GenericAvroDeserializer].getName
    )
    val range: Array[OffsetRange] = Array(
      OffsetRange.create(new TopicPartition(topic.topicName, 0), 0, 100))
    val p =
      (topic.schemaRegistrySettings.props ++ deser).mapValues[Object](identity).asJava
    // def decoder: Array[Byte] => Payment =
    KafkaUtils
      .createRDD[Array[Byte], Array[Byte]](
        spark.sparkContext,
        p,
        range,
        LocationStrategies.PreferConsistent)
      .map { x =>
        abc.decoder(x.value())
      }
      .toDS()
  }
}
