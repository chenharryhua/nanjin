package com.github.chenharryhua.nanjin.kafka

import java.time.{Instant, LocalDateTime}

import cats.derived.auto.show._
import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import io.chrisdavenport.cats.time._

final case class Employee(name: String, age: Int, department: String)
final case class ComplexMessage(
  a: Int        = 0,
  b: String     = "a",
  c: Float      = 1.0f,
  d: BigDecimal = 2.0,
  e: Double     = 3.0d,
  f: LocalDateTime,
  g: Instant,
  h: Colorish,
  i: Materials.Materials,
  j: Employee
)

class ComplexMessageTest extends AnyFunSuite {

  val topic: KafkaTopic[IO, Int, ComplexMessage] =
    ctx.topic[Int, ComplexMessage]("complex-msg-test")
  test("send complex message") {
    val ret = topic.schemaRegistry.register >>
      topic.producer.send(
        10,
        ComplexMessage(
          f = LocalDateTime.now,
          g = Instant.now(),
          h = Colorish.Red,
          i = Materials.Wood,
          j = Employee("banner", 10, "dev")))
    ret.unsafeRunSync()
  }

  test("read complex message") {
    topic.monitor.watchFromEarliest.unsafeRunSync()
  }
}
