package example.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.streaming.StreamsSerde
import example.topics.{barTopic, fooTopic}
import example.{ctx, Bar, Foo}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.Random

@DoNotDiscover
class ExampleKafkaKStream extends AnyFunSuite {
  test("kafka streaming") {
    def top(sb: StreamsBuilder, ksb: StreamsSerde): Unit = {
      import ksb.implicits.*

      sb.stream[Int, Foo](fooTopic.topicName.name.value)
        .flatMapValues(Option(_).map(foo => Bar(Random.nextInt(), foo.a.toLong)))
        .to(barTopic.topicName.name.value)
    }

    ctx.buildStreams("app")(top).stateUpdatesStream.interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }
}
