package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaTopicName._
import com.github.chenharryhua.nanjin.kafka.{
  KAvro,
  KafkaContext,
  KafkaSettings,
  KafkaTopic,
  ShowKafkaMessage
}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.avro._
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies, OffsetRange}
import com.sksamuel.avro4s
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, Record, RecordFormat, ToRecord}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Try

object SparkMain extends IOApp {

  val ctx =
    KafkaSettings.empty.brokers("localhost:9092").schemaRegistryUrl("http://localhost:8081").context

  val topic: KafkaTopic[Array[Byte], KAvro[Payment]] =
    topic"cc_payments".in[Array[Byte], KAvro[Payment]](ctx)
  val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  val avro  = topic.recordDecoder
  override def run(args: List[String]): IO[ExitCode] = {
    import spark.implicits._

    IO {
      val ds =
        new SparkafkaApiImpl(spark).dataset[Payment](topic.topicName.value, RecordFormat[Payment])
      ds.show()
      Row
    } >> //IO.sleep(40.seconds) >> IO { spark.stop() }
      IO(ExitCode.Success)
  }
}
