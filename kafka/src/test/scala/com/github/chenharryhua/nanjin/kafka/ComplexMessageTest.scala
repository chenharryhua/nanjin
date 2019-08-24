package com.github.chenharryhua.nanjin.kafka

import java.time.{Instant, LocalDateTime}

import cats.implicits._
import cats.derived.auto.show._
import cats.effect.IO
import org.scalatest.FunSuite
import io.chrisdavenport.cats.time._
final case class ShowableMessage(
  a: Int        = 0,
  b: String     = "a",
  c: Float      = 1.0f,
  d: BigDecimal = 2.0,
  e: Double     = 3.0d,
  f: LocalDateTime,
  g: Instant,
  h: Colorish,
  i: Materials.Materials
)

class ComplexMessageTest extends FunSuite {

  val topic: KafkaTopic[IO, Int, ShowableMessage] =
    ctx.topic[Int, ShowableMessage]("complex-msg-test")
  test("send complex message") {
    val ret = topic.schemaRegistry.register >>
      topic.producer.send(
        10,
        ShowableMessage(
          f = LocalDateTime.now,
          g = Instant.now(),
          h = Colorish.Red,
          i = Materials.Wood))
    ret.unsafeRunSync()
  }
  test("read complex message") {
    topic.monitor.watchFromEarliest.unsafeRunSync()
  }
}
