package mtest.kafka

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec.NJSerde
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef, TopicName}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.scalatest.funsuite.AnyFunSuite

object KafkaStreamingCases {

  case class StreamOneValue(name: String, size: Int)

  case class StreamTwoValue(name: String, color: Int)

  case class StreamKey(name: Int)

  case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)

}

class KafkaStreamingTest extends AnyFunSuite {
  import KafkaStreamingCases._

  val one: KafkaTopic[IO, StreamKey, StreamOneValue] =
    TopicDef[StreamKey, StreamOneValue](TopicName("stream-one")).in(ctx)

  val two: KafkaTopic[IO, StreamKey, StreamTwoValue] =
    TopicDef[StreamKey, StreamTwoValue](TopicName("stream-two")).in(ctx)

  val tgt: KafkaTopic[IO, StreamKey, StreamTarget] =
    TopicDef[StreamKey, StreamTarget](TopicName("stream-target")).in(ctx)
  implicit val oneKey: NJSerde[StreamKey]        = one.codec.keySerde
  implicit val oneValue: NJSerde[StreamOneValue] = one.codec.valSerde
  implicit val twoValue: NJSerde[StreamTwoValue] = two.codec.valSerde
  implicit val tgtValue: NJSerde[StreamTarget]   = tgt.codec.valSerde

  ignore("generate data") {
    (one.schemaRegister >> two.schemaRegister).unsafeRunSync()
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
