package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.KafkaSettings
import com.github.chenharryhua.nanjin.kafka.KafkaTopicName._
import org.apache.spark.sql.SparkSession

object SparkMain extends IOApp {

  val ctx =
    KafkaSettings.empty.brokers("localhost:9092").schemaRegistryUrl("http://localhost:8081").context
  val topic = topic"nyc_yellow_taxi_trip_data".in[Int, Int](ctx)
  val spark = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  override def run(args: List[String]): IO[ExitCode] = {
    import spark.implicits._
    IO {
      val ggg = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic.topicName.value)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
      ggg.show
    } >>
      IO(ExitCode.Success)
  }
}
