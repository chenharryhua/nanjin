package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.sparkafka.{SparkConsumerRecord, SparkafkaStream}
import org.apache.spark.sql.Dataset
import org.scalatest.FunSuite

class SparkStreamingTest extends FunSuite {
  test("run streaming") {
    spark.use { s =>
      val df  = SparkafkaStream.sstream(s, topics.pencil_topic)
      val kdf = df.dataset.writeStream.outputMode("append").format("console")
      IO(kdf.start()) >> IO.never //.map(println)
    }.unsafeRunSync()
  }
}
