package com.github.chenharryhua.nanjin.sparkafka

import com.github.chenharryhua.nanjin.kafka.{KAvro, KafkaAvroSerde, ValueSerde}
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.spark.sql.avro._
import com.sksamuel.avro4s.{Encoder => AEncoder, Decoder => ADecoder}

trait SparkafkaApi {}

final class SparkafkaApiImpl(spark: SparkSession) extends SparkafkaApi with Serializable {
  import spark.implicits._
  @transient private lazy val csr = new CachedSchemaRegistryClient("http://localhost:8081", 100)

  def valueDataset[V: Encoder: ClassTag](
    props: Map[String, Object],
    range: Array[OffsetRange]): Dataset[V] =
    KafkaUtils
      .createRDD[Array[Byte], V](
        spark.sparkContext,
        props.asJava,
        range,
        LocationStrategies.PreferConsistent)
      .map(_.value())
      .toDS()

}
