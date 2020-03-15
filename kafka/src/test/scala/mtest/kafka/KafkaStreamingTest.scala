package mtest.kafka

import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import io.chrisdavenport.cats.time._
import org.apache.kafka.streams.scala.ImplicitConversions._
import com.github.chenharryhua.nanjin.kafka.TopicName


object KafkaStreamingCases {

  case class StreamOneValue(name: String, size: Int)

  case class StreamTwoValue(name: String, color: Int)

  case class StreamKey(name: Int)

  case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

}

class KafkaStreamingTest extends AnyFunSuite {
  import KafkaStreamingCases._
  val one               = ctx.topic[StreamKey, StreamOneValue](TopicName("stream-one"))
  val two               = ctx.topic[StreamKey, StreamTwoValue](TopicName("stream-two"))
  val tgt               = ctx.topic[StreamKey, StreamTarget](TopicName("stream-target"))
  implicit val oneKey   = one.codec.keySerde
  implicit val oneValue = one.codec.valSerde
  implicit val twoValue = two.codec.valSerde
  implicit val tgtValue = tgt.codec.valSerde

  ignore("generate data") {
    (one.schemaRegistry.register >> two.schemaRegistry.register).unsafeRunSync()
    fs2.Stream
      .range(0, 100)
      .covary[IO]
      .evalMap { i =>
        one.send(StreamKey(i), StreamOneValue("one", i)) >> two.send(
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
