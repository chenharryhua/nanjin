package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite

sealed trait Color
final case class Red(str: String, i: Int) extends Color
final case class Blue(str: String) extends Color
final case class Yellow(str: String) extends Color
final case class Cloth(color: Color, name: String, size: Int)

class KAvroTest extends FunSuite with ShowKafkaMessage {
  test("should be able to send avro") {
    val topic = ctx.topic[String, Payment]("cc_payment")
    val p     = Payment("a", "b", 0, "c", "cc", 10L)
    topic.producer.send("abc", p).unsafeRunSync()
  }
  test("should support coproduct") {
    val topic = ctx.topic[Int, Cloth]("cloth")
    val b     = Cloth(Blue("b"), "blue-cloth", 1)
    val r     = Cloth(Red("r", 1), "red-cloth", 2)
    val y     = Cloth(Yellow("y"), "yellow-cloth", 3)
    val run = topic.producer.send(2, r) >>
      topic.producer.send(3, y) >>
      topic.producer.send(1, b) >>
      topic.consumer.retrieveLastMessages
    assert(run.unsafeRunSync().head.value().get === b)
  }
}
