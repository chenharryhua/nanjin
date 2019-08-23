package com.github.chenharryhua.nanjin.kafka

import java.time.LocalDateTime

import cats.effect.IO
import org.scalatest.FunSuite
import frameless.cats.implicits._

class SerializableTest extends FunSuite {
  test("serizable") {
    val end   = LocalDateTime.now()
    val start = end.minusYears(1)
    sparkSession.use { s =>
      Sparkafka.dataset(s, taxi, start, end).flatMap(_.show[IO]())
    }.map(println).unsafeRunSync
  }
}
