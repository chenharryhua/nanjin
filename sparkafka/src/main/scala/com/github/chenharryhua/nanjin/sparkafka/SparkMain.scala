package com.github.chenharryhua.nanjin.sparkafka

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.{KafkaSettings, KafkaTopic}
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.experimental.macros

object SparkMain extends IOApp {

  val ctx = KafkaSettings.local.ioContext

  val topic =
    // ctx.topic[String, String]("au.marketing.promotion.prod.customer-offer-status.stream.json")
    ctx.topic[Array[Byte], Payment]("cc_payments")

  val nyc_yellow_taxi_trip_data = ctx.topic[Array[Byte], trip_record]("nyc_yellow_taxi_trip_data")

  val spark: SparkSession =
    SparkSession.builder().master("local[*]").appName("test").getOrCreate()
  
    import spark.implicits._  

  override def run(args: List[String]): IO[ExitCode] = {
    val api = new SparkafkaApiImpl(spark)

//    IO(personDF.writeStream.format("console").outputMode("append").start().awaitTermination()) >>
    IO(api.valueDataset(topic).show()) >>
      IO(ExitCode.Success)
  }

  def df: DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("kafka.schema.registry.url", "http://localhost:8081")
      .option("subscribe", "nyc_yellow_taxi_trip_data")
      .option("startingOffsets", "earliest") // From starting
      .load()

  def personDF =
    df.select(from_avro(col("value"), nyc_yellow_taxi_trip_data.valueSerde.schema.toString()))
}
