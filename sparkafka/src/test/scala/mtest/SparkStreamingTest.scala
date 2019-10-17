package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.sparkafka.{SparKafkaConsumerRecord, SparKafkaStream}
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.scalatest.FunSuite
import frameless.cats.implicits._
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class SparkStreamingTest extends AnyFunSuite {
  ignore("run streaming") {
    spark.use { s =>
      import s.spark
      val df = SparKafkaStream.sstream(topics.pencil_topic)
      val kdf: DataStreamWriter[SparKafkaConsumerRecord[Int, Pencil]] = df.dataset.writeStream
        .trigger(Trigger.ProcessingTime(60.seconds))
        .option("checkpointLocation", "test-data/checkpoint-test")
        .outputMode("append")
        .format("console")
      SparKafkaStream.start[IO, SparKafkaConsumerRecord[Int, Pencil]](kdf)
    }.unsafeRunSync()
  }
}
