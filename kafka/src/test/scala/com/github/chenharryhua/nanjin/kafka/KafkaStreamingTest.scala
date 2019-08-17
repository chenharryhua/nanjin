package com.github.chenharryhua.nanjin.kafka

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.scalatest.FunSuite
case class StreamOneValue(name: String, size: Int)
case class StreamTwoValue(name: String, color: Int)
case class StreamKey(name: String)

case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

class KafkaStreamingTest extends FunSuite {
  val one               = ctx.topic[StreamKey, StreamOneValue]("stream-one")
  val two               = ctx.topic[StreamKey, StreamTwoValue]("stream-two")
  val tgt               = ctx.topic[StreamKey, StreamTarget]("stream-target")
  implicit val oneKey   = one.keySerde
  implicit val oneValue = one.valueSerde
  implicit val twoValue = two.valueSerde
  implicit val tgtValue = tgt.valueSerde

  test("streaming") {
    val top = for {
      a <- one.kafkaStream.ktable
      b <- two.kafkaStream.ktable
    } yield a
      .join(b)((v1, v2) => StreamTarget(v1.name, v2.name, v1.size, v2.color))
      .toStream
      .to(tgt)
    ctx.kafkaStreams(top).compile.drain.unsafeRunSync()
  }
}
