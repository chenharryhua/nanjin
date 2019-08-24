package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.scalatest.FunSuite
import cats.implicits._
import cats.derived.auto.show._
case class StreamOneValue(name: String, size: Int)
case class StreamTwoValue(name: String, color: Int)
case class StreamKey(name: Int)

case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

class KafkaStreamingTest extends FunSuite {
  val one               = ctx.topic[StreamKey, StreamOneValue]("stream-one")
  val two               = ctx.topic[StreamKey, StreamTwoValue]("stream-two")
  val tgt               = ctx.topic[StreamKey, StreamTarget]("stream-target")
  implicit val oneKey   = one.keySerde
  implicit val oneValue = one.valueSerde
  implicit val twoValue = two.valueSerde
  implicit val tgtValue = tgt.valueSerde
  test("generate data") {
    (one.schemaRegistry.register >> two.schemaRegistry.register).unsafeRunSync()
    fs2.Stream
      .range(0, 100)
      .covary[IO]
      .evalMap { i =>
        one.producer.send(StreamKey(i), StreamOneValue("one", i)) >> two.producer.send(
          StreamKey(i),
          StreamTwoValue("two", i))
      }
      .compile
      .drain
      .flatTap(_ => IO(println("data injection was completed")))
      .unsafeRunSync()
  }
  ignore("streaming") {
    val top = for {
      a <- one.kafkaStream.kstream
      b <- two.kafkaStream.ktable
    } yield a
      .join(b)((v1, v2) => StreamTarget(v1.name, v2.name, v1.size, v2.color))
      // .toStream
      .to(tgt)
    (ctx.kafkaStreams(top) >> fs2.Stream.never[IO]).compile.drain.unsafeRunSync()
  }
}
