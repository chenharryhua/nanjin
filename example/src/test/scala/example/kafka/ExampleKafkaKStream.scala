package example.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.streaming.StreamsSerde
import example.topics.{barTopic, fooTopic}
import example.{ctx, Bar, Foo}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random

@DoNotDiscover
class ExampleKafkaKStream extends AnyFunSuite {
  test("kafka streaming") {
    def top(sb: StreamsBuilder, ksb: StreamsSerde): Unit = {
      implicit val con: Consumed[Int, Foo] = ksb.consumed[Int, Foo]
      implicit val pro: Produced[Int, Bar] = ksb.produced[Int, Bar]
      sb.stream[Int, Foo](fooTopic.topicDef.topicName.value)
        .mapValues(foo => Bar(Random.nextInt(), foo.a.toLong))
        .to(barTopic.topicName.value)
    }

    ctx.buildStreams("app")(top).stateUpdates.interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }
}
