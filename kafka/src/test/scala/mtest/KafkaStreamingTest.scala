package mtest

import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import cats.derived.auto.show._ 
import io.chrisdavenport.cats.time._
import org.apache.kafka.streams.scala.ImplicitConversions._

case class StreamOneValue(name: String, size: Int)
case class StreamTwoValue(name: String, color: Int)
case class StreamKey(name: Int)

case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

class KafkaStreamingTest extends AnyFunSuite {
  val one               = ctx.topic[StreamKey, StreamOneValue]("stream-one")
  val two               = ctx.topic[StreamKey, StreamTwoValue]("stream-two")
  val tgt               = ctx.topic[StreamKey, StreamTarget]("stream-target")
  implicit val oneKey   = one.keyCodec.serde
  implicit val oneValue = one.valueCodec.serde
  implicit val twoValue = two.valueCodec.serde
  implicit val tgtValue = tgt.valueCodec.serde
  ignore("generate data") {
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
