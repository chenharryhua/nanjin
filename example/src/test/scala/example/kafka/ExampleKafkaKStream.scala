package example.kafka

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import example.topics.{barTopic, fooTopic}
import example.{ctx, Bar}
import fs2.Stream
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions.*
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random
import com.github.chenharryhua.nanjin.kafka.streaming.KafkaStreamingProduced

@DoNotDiscover
class ExampleKafkaKStream extends AnyFunSuite {
  test("kafka streaming") {
    implicit val bar: KafkaStreamingProduced[IO,Int,Bar] = barTopic.asProducer
    val top: Kleisli[Id, StreamsBuilder, Unit] =
      fooTopic.asConsumer.kstream.map(_.mapValues(foo => Bar(Random.nextInt(), foo.a.toLong)).to(bar))

    ctx.buildStreams(top).stream.interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }
}
