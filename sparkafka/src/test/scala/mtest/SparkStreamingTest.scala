package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.NJConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.FunSuite
import cats.implicits._

class SparkStreamingTest extends FunSuite {
  //val topic = topics.pencil_topic
  test("run streaming") {

    spark.use { s =>
      import s.implicits._
      val df = s.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topics.pencil_topic.topicName)
        .load()
        .selectExpr("CAST(key AS Binary)", "CAST(value AS Binary)")
        .as[(Array[Byte], Array[Byte])]
        .map(msg =>
          msg.bimap(topics.pencil_topic.keyIso.get, topics.pencil_topic.valueIso.get)._2.name)
        .as[String]

      val kdf = df.writeStream.outputMode("append").format("console")

      IO(kdf.start()) >> IO.never //.map(println)
    }.unsafeRunSync()
  }
}
