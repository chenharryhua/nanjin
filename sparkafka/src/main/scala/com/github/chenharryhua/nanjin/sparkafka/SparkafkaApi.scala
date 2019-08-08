package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, Transformers}
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
trait SparkafkaApi extends Serializable {}

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
      "key.deserializer" -> topic.keySerde.deserializer.getClass.getName,
      "value.deserializer" -> gv.deserializer.getClass.getName // classOf[GenericAvroDeserializer].getName
    )
    val range: Array[OffsetRange] = Array(
      OffsetRange.create(new TopicPartition(topic.topicName, 0), 0, 100))
    val p =
      (topic.schemaRegistrySettings.props ++ deser).mapValues[Object](identity).asJava

    val trans = Transformers.transform[V](RecordFormat[V])

    KafkaUtils
      .createRDD[K, GenericRecord](
        spark.sparkContext,
        p,
        range,
        LocationStrategies.PreferConsistent)
      .flatMap { x =>
        val s = x.value.toString
        println(s)
        decode[Payment](x.value().toString).toOption
      }
      .toDS()
  }
}
