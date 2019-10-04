package mtest

import java.time.LocalDateTime

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.sparkafka._
import com.github.chenharryhua.nanjin.codec._
import com.github.chenharryhua.nanjin.kafka._
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class SparkTest extends AnyFunSuite with Aggregations {
  val end   = LocalDateTime.now()
  val start = end.minusYears(1)
  test("should be able to show topic data") {
    spark.use { implicit s =>
      Sparkafka.valueDataset(topics.taxi, start, end).flatMap(_.show[IO](1)) >>
        Sparkafka.dataset(topics.taxi, start, end).flatMap(_.show[IO](1)) >>
        Sparkafka.safeDataset(topics.taxi, start, end).flatMap(_.show[IO](1)) >>
        Sparkafka.safeValueDataset(topics.taxi, start, end).flatMap(_.show[IO](2))
    }.unsafeRunSync
  }
  test("should be able to save topic to json") {
    spark.use { implicit s =>
      Sparkafka
        .dataset(topics.taxi, start, end)
        .map(
          _.dataset
            .filter(_.value.VendorID == 2)
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .json("test-data/json"))
    }.unsafeRunSync()
  }
  test("should be able to save topic to parquet") {
    spark.use { implicit s =>
      Sparkafka
        .dataset(topics.taxi, start, end)
        .map(
          _.dataset
            .filter(_.value.VendorID == 2)
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .parquet("test-data/parquet"))
    }.unsafeRunSync()
  }

  test("stats") {
    spark.use { implicit s =>
      topics.taxi.topicDataset.dateset
    }.unsafeRunSync
  }
}
