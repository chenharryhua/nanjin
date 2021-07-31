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

@DoNotDiscover
class ExampleKafkaKStream extends AnyFunSuite {
  test("kafka streaming") {
    implicit val keySerde: Serde[Bar]   = barTopic.codec.valSerde
    implicit val valSerde: SerdeOf[Int] = barTopic.codec.keySerde
    val top: Kleisli[Id, StreamsBuilder, Unit] =
      fooTopic.kafkaStream.kstream.map(_.mapValues(foo => Bar(Random.nextInt(), foo.a.toLong)).to(barTopic))

    ctx.buildStreams(top).stream.interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }
}
