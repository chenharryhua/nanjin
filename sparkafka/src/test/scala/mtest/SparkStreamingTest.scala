package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.sparkafka.SparkafkaStream
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.FunSuite
import frameless.cats.implicits._
import scala.concurrent.duration._

class SparkStreamingTest extends FunSuite {
  test("run streaming") {
    spark.use { implicit s =>
      val df = SparkafkaStream.sstream(topics.pencil_topic)
      val kdf = df.dataset.writeStream
        .trigger(Trigger.ProcessingTime(60.seconds))
        .option("checkpointLocation", "checkpoint-test")
        .outputMode("append")
        .format("console")
      IO(kdf.start()) >> IO.never //.map(println)
    }.unsafeRunSync()
  }
}
