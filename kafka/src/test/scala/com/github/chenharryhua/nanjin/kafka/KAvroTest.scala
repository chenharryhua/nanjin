package com.github.chenharryhua.nanjin.kafka

import cats.implicits._
import org.scalatest.FunSuite

sealed trait Color
final case class Red(str: String, i: Int) extends Color
final case class Blue(str: String) extends Color
final case class Yellow(str: String) extends Color
final case class Cloth(color: Color)

class KAvroTest extends FunSuite with ShowKafkaMessage {
  test("send avro") {
    val topic = ctx.topic[String, Payment]("cc_payment")
    val p     = Payment("a", "b", 0, "c", "cc", 10L)
    topic.producer.send("abc", p).unsafeRunSync()
  }
  test("send color") {
    val topic = ctx.topic[Int, Cloth]("cloth")
    val b     = Cloth(Blue("b"))
    val r     = Cloth(Red("r", 1))
    val y     = Cloth(Yellow("y"))
    // val run   = topic.producer.send(2, r) >> topic.producer.send(3, y) >> topic.producer.send(1, b)
    // run.unsafeRunSync
    val ret = topic.consumer.retrieveLastMessages.map(_.show).unsafeRunSync
    println(ret)
    topic.schemaRegistry.latestMeta.map(_.show).map(println).unsafeRunSync
  }
}
