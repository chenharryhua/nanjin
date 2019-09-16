package mtest
import java.time.LocalDateTime

import cats.derived.auto.show._
import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.sparkafka.{Aggregations, SparkafkaDataset}
import frameless.cats.implicits._
import fs2.Chunk
import org.scalatest.FunSuite
import org.scalatest.funsuite.AnyFunSuite

class EnumTest extends AnyFunSuite with Aggregations {
  topics.pencil_topic.schemaRegistry.register.unsafeRunSync()
  val end   = LocalDateTime.now()
  val start = end.minusHours(1)

  val pencils =
    List(
      (10, Pencil("steal2", Colorish.Red)),
      (20, Pencil("wood2", Colorish.Green)),
      (30, Pencil("plastic2", Colorish.Blue)))
  topics.pencil_topic.producer.send(pencils).unsafeRunSync()

  test("should be able to process enum data") {

    fs2.Stream
      .eval(spark.use { implicit s =>
        SparkafkaDataset
          .dataset(topics.pencil_topic, start, end)
          .flatMap(_.take[IO](10))
          .map(Chunk.seq)
      })
      .flatMap(fs2.Stream.chunk)
      .map(_.show)
      .showLinesStdOut
      .compile
      .drain
      .unsafeRunSync()

  }

  test("same key should go to same partition") {
    spark.use { implicit s =>
      SparkafkaDataset
        .dataset(topics.pencil_topic, end.minusYears(3), end)
        .map(_.keysInPartitions)
        .flatMap(_.show[IO]())
    }.unsafeRunSync
  }
}
