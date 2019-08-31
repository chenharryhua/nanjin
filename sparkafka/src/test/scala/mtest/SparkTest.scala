package mtest

import java.time.LocalDateTime

import cats.effect.IO
import com.github.chenharryhua.nanjin.sparkafka.{Aggregations, SparkafkaDataset}
import org.apache.spark.sql.SaveMode
import org.scalatest.FunSuite
import cats.implicits._
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.scalatest.funsuite.AnyFunSuite

class SparkTest extends AnyFunSuite with Aggregations {
  val end   = LocalDateTime.now()
  val start = end.minusYears(1)
  test("should be able to show topic data") {
    spark.use { implicit s =>
      SparkafkaDataset.valueDataset(topics.taxi, start, end).flatMap(_.show[IO](1)) >>
        SparkafkaDataset.dataset(topics.taxi, start, end).flatMap(_.show[IO](1)) >>
        SparkafkaDataset.safeDataset(topics.taxi, start, end).flatMap(_.show[IO](1)) >>
        SparkafkaDataset.safeValueDataset(topics.taxi, start, end).flatMap(_.show[IO](2))
    }.unsafeRunSync
  }
  test("should be able to save topic to json") {
    spark.use { implicit s =>
      SparkafkaDataset
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
      SparkafkaDataset
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
      SparkafkaDataset.dataset(topics.taxi, start, end).flatMap(_.hourly.show[IO]())
    }.unsafeRunSync
  }
}
