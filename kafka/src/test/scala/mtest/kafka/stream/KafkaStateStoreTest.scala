package mtest.kafka.stream

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{KafkaStore, StoreName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import fs2.Stream
import mtest.kafka._
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.state.Stores
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class KafkaStateStoreTest extends AnyFunSuite {

  test("state store") {
    val store = new KafkaStore.InMemory[Int, Int](StoreName("in.memory.store"), SerdeOf[Int], SerdeOf[Int])
    val topic = ctx.topic[Int, Int]("in.memory.state.test.topic")
    val top   = topic.kafkaStream.kstream.map(_.print(Printed.toSysOut()))

    val top2 = for {
      s <- topic.kafkaStream.kstream
    } yield s.to(topic)
    ctx.buildStreams(top)
  }
}
