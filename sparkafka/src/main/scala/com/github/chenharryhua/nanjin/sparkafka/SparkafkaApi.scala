package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.kafka.{IoKafkaContext, KafkaTopic}
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.sql
import com.sksamuel.avro4s
import com.sksamuel.avro4s.RecordFormat

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerde}
import io.circe.generic.auto._
import io.circe.generic
import io.circe.syntax._
import io.circe.parser.decode
import org.apache.kafka.common.serialization.ByteArrayDeserializer
trait SparkafkaApi extends Serializable {}

object abc {
  def decoder = SparkMain.topic.valueIso.get _
}

final class SparkafkaApiImpl(spark: SparkSession) extends SparkafkaApi {
  import spark.implicits._

  def valueDataset[
    F[_],
    K,
    V: sql.Encoder: ClassTag: avro4s.Encoder: avro4s.Decoder: avro4s.SchemaFor](
    topic: KafkaTopic[F, K, V]): Dataset[Payment] = {
    val gv = new GenericAvroSerde
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
