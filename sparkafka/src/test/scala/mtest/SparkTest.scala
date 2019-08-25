package mtest

import java.time.LocalDateTime

import cats.effect.IO
import com.github.chenharryhua.nanjin.sparkafka.SparkafkaDStream
import org.apache.spark.sql.SaveMode
import org.scalatest.FunSuite
import cats.implicits._
import frameless.cats.implicits.framelessCatsSparkDelayForSync

class SparkTest extends FunSuite with Serializable {
  val end   = LocalDateTime.now()
  val start = end.minusYears(1)
  test("should be able to show topic data") {
    spark.use { implicit s =>
      SparkafkaDStream.valueDataset(topics.taxi, start, end).flatMap(_.show[IO](3)) >>
        SparkafkaDStream.dataset(topics.taxi, start, end).flatMap(_.show[IO](3)) >>
        SparkafkaDStream.safeDataset(topics.taxi, start, end).flatMap(_.show[IO](3)) >>
        SparkafkaDStream.safeValueDataset(topics.taxi, start, end).flatMap(_.show[IO](3))
    }.unsafeRunSync
  }
  test("should be able to save topic to json") {
    spark.use { implicit s =>
      SparkafkaDStream
        .dataset(topics.taxi, start, end)
        .map(
          _.dataset
            .filter(_.value.VendorID == 2)
            .coalesce(1)
            .write
            .mode(SaveMode.Overwrite)
            .json("json-test"))
    }.unsafeRunSync()
  }
}
