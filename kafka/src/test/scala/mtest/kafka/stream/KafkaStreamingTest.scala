package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka._
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.{BeforeAndAfter, DoNotDiscover}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import cats.effect.unsafe.implicits.global

object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)

  case class TableTwo(name: String, color: Int)
  case class GlobalThree(name: String, weight: Int)

  case class StreamTarget(name: String, weight: Int, color: Int)

  val s1Topic: KafkaTopic[IO, Int, StreamOne]   = ctx.topic[Int, StreamOne]("stream.test.stream.one")
  val t2Topic: KafkaTopic[IO, Int, TableTwo]    = ctx.topic[Int, TableTwo]("stream.test.table.two")
  val g3Topic: KafkaTopic[IO, Int, GlobalThree] = ctx.topic[Int, GlobalThree]("stream.test.global.three")

  val tgt: KafkaTopic[IO, Int, StreamTarget] = ctx.topic[Int, StreamTarget]("stream.test.join.target")

  val sendS1Data: Stream[IO, ProducerResult[Unit, Int, StreamOne]] = Stream
    .fixedRate[IO](1.seconds)
    .zipRight(
      Stream
        .emits(List(
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 1, StreamOne("a", 0)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 2, StreamOne("b", 1)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 3, StreamOne("c", 2)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 4, StreamOne("d", 3)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 5, StreamOne("e", 4)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 101, StreamOne("na", -1)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 102, StreamOne("na", -1)),
          ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 103, StreamOne("na", -1))
        ).map(ProducerRecords.one(_)))
        .covary[IO]
        .through(s1Topic.fs2Channel.producerPipe)
        .debug())

  val sendT2Data: Stream[IO, ProducerResult[Unit, Int, TableTwo]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(t2Topic.topicName.value, 1, TableTwo("x", 0)),
        ProducerRecord(t2Topic.topicName.value, 2, TableTwo("y", 1)),
        ProducerRecord(t2Topic.topicName.value, 3, TableTwo("z", 2))
      ))).covary[IO].through(t2Topic.fs2Channel.producerPipe)

  val sendG3Data: Stream[IO, ProducerResult[Unit, Int, GlobalThree]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(g3Topic.topicName.value, 4, GlobalThree("gx", 1000)),
        ProducerRecord(g3Topic.topicName.value, 5, GlobalThree("gy", 2000)),
        ProducerRecord(g3Topic.topicName.value, 6, GlobalThree("gz", 3000))
      ))).covary[IO].through(g3Topic.fs2Channel.producerPipe)

  val expected: Set[StreamTarget] = Set(
    StreamTarget("a", 0, 0),
    StreamTarget("b", 0, 1),
    StreamTarget("c", 0, 2),
    StreamTarget("d", 1000, 0),
    StreamTarget("e", 2000, 0)
  )
}

@DoNotDiscover
class KafkaStreamingTest extends AnyFunSuite with BeforeAndAfter {
  import KafkaStreamingData._

  implicit val oneValue: Serde[StreamOne]    = s1Topic.codec.valSerde
  implicit val twoValue: Serde[TableTwo]     = t2Topic.codec.valSerde
  implicit val tgtValue: Serde[StreamTarget] = tgt.codec.valSerde

  before(sendT2Data.concurrently(sendG3Data).compile.drain.unsafeRunSync())

  test("stream-table join") {

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.kafkaStream.kstream
      b <- t2Topic.kafkaStream.ktable
      c <- g3Topic.kafkaStream.gktable
    } yield {
      a.join(c)((i, s1) => i, (s1, g3) => StreamTarget(s1.name, g3.weight, 0)).to(tgt)
      a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color)).to(tgt)
    }

    val harvest: Stream[IO, StreamTarget] =
      tgt.fs2Channel.stream
        .map(x => tgt.decoder(x).decode)
        .observe(_.map(_.offset).through(commitBatchWithin[IO](3, 1.seconds)).printlns)
        .map(_.record.value)

    val runStream: Stream[IO, StreamTarget] =
      harvest
        .concurrently(sendS1Data)
        .concurrently(
          ctx.buildStreams(top).run.handleErrorWith(_ => Stream.sleep[IO](2.seconds) >> ctx.buildStreams(top).run) >>
            Stream.never[IO])
        .interruptAfter(15.seconds)

    val res: Set[StreamTarget] = runStream.compile.toList.unsafeRunSync().toSet 
    println(res)
    assert(res == expected)
  }
}
