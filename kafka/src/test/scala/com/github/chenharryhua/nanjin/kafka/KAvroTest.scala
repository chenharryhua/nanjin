package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite
import cats.derived.auto.show._
sealed trait Color
final case class Red(str: String, i: Int) extends Color
final case class Green(str: String) extends Color
final case class Blue(str: String) extends Color
final case class Cloth(color: Color, name: String, size: Int)

class KAvroTest extends FunSuite with ShowKafkaMessage {
  test("should support coproduct") {
    val topic = ctx.topic[Int, Cloth]("cloth")
    val b     = Cloth(Blue("b"), "blue-cloth", 1)
    val r     = Cloth(Red("r", 1), "red-cloth", 2)
    val g     = Cloth(Green("g"), "green-cloth", 3)
    val run =
      topic.producer.send(1, r) >>
        topic.producer.send(2, g) >>
        topic.producer.send(3, b) >>
        topic.consumer.retrieveLastRecords.map(m => topic.decode(m.head))
    assert(run.unsafeRunSync().value() === b)
  }
}
