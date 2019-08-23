package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.effect.IO
import org.scalatest.FunSuite
import frameless.cats.implicits._

class SerializableTest extends FunSuite with Serializable {
  test("serizable") {
    val end   = LocalDateTime.now()
    val start = end.minusYears(1)
    sparkSession.use { implicit s =>
      //Sparkafka.dataset(s, taxi, start, end).flatMap(_.show[IO]())
      SparkafkaDataset.safeDataset(taxi, start, end).flatMap(_.show[IO]())
      //  taxi.dataset(start, end).flatMap(_.show[IO]())
    }.unsafeRunSync
  }
}
