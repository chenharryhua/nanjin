package example.kafka

import cats.Id
import cats.data.Kleisli
import cats.effect.unsafe.implicits.global
import example.topics.{barTopic, fooTopic}
import example.{ctx, Bar}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random

@DoNotDiscover
class ExampleKafkaKStream extends AnyFunSuite {
  test("kafka streaming") {
    val top: Kleisli[Id, StreamsBuilder, Unit] =
      fooTopic.asConsumer.kstream.map(
        _.mapValues(foo => Bar(Random.nextInt(), foo.a.toLong))
          .to(barTopic.topicName.value)(barTopic.asProduced))

    ctx.buildStreams("app", top).stateUpdates.interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }
}
