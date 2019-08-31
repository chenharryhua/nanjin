package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.sparkafka.{SparkConsumerRecord, SparkafkaStream}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.scalatest.FunSuite
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class SparkStreamingTest extends AnyFunSuite {
  ignore("run streaming") {
    spark.use { implicit s =>
      val df = SparkafkaStream.sstream(topics.pencil_topic)
      val kdf: DataStreamWriter[SparkConsumerRecord[Int, Pencil]] = df.dataset.writeStream
        .trigger(Trigger.ProcessingTime(60.seconds))
        .option("checkpointLocation", "test-data/checkpoint-test")
        .outputMode("append")
        .format("console")
      SparkafkaStream.start[IO, SparkConsumerRecord[Int, Pencil]](kdf)
    }.unsafeRunSync()
  }
}
