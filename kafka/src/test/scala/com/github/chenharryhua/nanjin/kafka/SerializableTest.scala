package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.effect.IO
import org.scalatest.FunSuite
import frameless.cats.implicits._
import cats.implicits._

class SerializableTest extends FunSuite with Serializable {
  test("serizable") {
    val end   = LocalDateTime.now()
    val start = end.minusYears(1)
    sparkSession.use { implicit s =>
      //Sparkafka.dataset(s, taxi, start, end).flatMap(_.show[IO]())
      //  SparkafkaDataset.checkSameKeyInSamePartition(taxi, start, end).map(println)
      SparkafkaDataset.valueDataset(taxi, start, end).flatMap(_.show[IO](3)) >>
        SparkafkaDataset.dataset(taxi, start, end).flatMap(_.show[IO](3)) >>
        SparkafkaDataset.safeDataset(taxi, start, end).flatMap(_.show[IO](3)) >>
        SparkafkaDataset.safeValueDataset(taxi, start, end).flatMap(_.show[IO](3))
    }.unsafeRunSync
  }
}
