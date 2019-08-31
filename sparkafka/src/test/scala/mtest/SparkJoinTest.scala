package mtest

import java.time.LocalDateTime

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.codec.{utils, NJProducerRecord}
import com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
import frameless.cats.implicits._
import fs2.Stream
import org.scalatest.FunSuite
import org.scalatest.funsuite.AnyFunSuite

class SparkJoinTest extends AnyFunSuite {
  val num: Int    = 100
  val coefficient = 1000000
  val start       = LocalDateTime.now.minusNanos(num.toLong * coefficient)
  println(s"start-time: $start")

  val first_data =
    Stream
      .range(0, num)
      .covary[IO]
      .map(
        i =>
          NJProducerRecord[Int, FirstStream](
            topics.first_topic.topicName,
            0,
            FirstStream(i.toString, utils.random4d.value)).updateTimestamp(
            Some(utils.localDateTime2KafkaTimestamp(start.plusNanos(i.toLong * coefficient)))))
      .evalMap(msg => topics.first_topic.producer.send(msg))

  val second_data =
    Stream
      .range(0, num)
      .covary[IO]
      .map(
        i =>
          NJProducerRecord[Int, SecondStream](
            topics.second_topic.topicName,
            0,
            SecondStream(i.toString, utils.random4d.value)).updateTimestamp(
            Some(utils.localDateTime2KafkaTimestamp(start.plusNanos(i.toLong * coefficient)))))
      .evalMap(msg => topics.second_topic.producer.send(msg))

  test("gen data") {
    println(topics.first_topic.kafkaProducerSettings.show)
    (first_data.compile.drain >> second_data.compile.drain >> IO(println("injection completed")))
      .unsafeRunSync()
  }

  test("should be able to join two topics together") {

    spark.use { implicit s =>
      for {
        f <- SparkafkaDataset.valueDataset(
          topics.first_topic,
          start,
          start.plusNanos(num.toLong * coefficient))
        s <- SparkafkaDataset.valueDataset(
          topics.second_topic,
          start,
          start.plusNanos(num.toLong * coefficient))
        _ <- f.show[IO]()
        _ <- s.show[IO]()
        cf <- f.count[IO]()
        cs <- s.count[IO]
        ds = f.joinInner(s)(f('name) === s('name))
        cds <- ds.count[IO]()
        _ <- ds.show[IO]()
      } yield println(s"result = $cds cf = $cf, cs = $cs")

    }.unsafeRunSync()
  }
}
