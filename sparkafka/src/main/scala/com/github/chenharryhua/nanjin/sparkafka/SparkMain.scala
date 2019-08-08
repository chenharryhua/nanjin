package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.TopicDef._
import com.github.chenharryhua.nanjin.kafka.{
  KafkaContext,
  KafkaSettings,
  KafkaTopic,
  ShowKafkaMessage,
  TopicDef
}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import com.sksamuel.avro4s
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, Record, RecordFormat, ToRecord}
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.sql.functions.col

object SparkMain extends IOApp {

  val ctx = KafkaSettings.local.ioContext

  val topic =
    // ctx.topic[String, String]("au.marketing.promotion.prod.customer-offer-status.stream.json")
    ctx.topic[Array[Byte], Payment]("cc_payments")

  val spark: SparkSession =
    SparkSession.builder().master("local[*]").appName("test").getOrCreate()

  override def run(args: List[String]): IO[ExitCode] = {
    import spark.implicits._
    val api = new SparkafkaApiImpl(spark)
    val df: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "cc_payments")
      .option("startingOffsets", "earliest") // From starting
      .load()
    val personDF =
      df.select(from_avro(col("value"), topic.valueSerde.schema.toString()))

    IO(personDF.writeStream.format("console").outputMode("append").start().awaitTermination()) >>
      //  IO(api.valueDataset(topic).show()) >>
      IO(ExitCode.Success)
  }
}
