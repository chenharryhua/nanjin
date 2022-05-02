package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import com.github.chenharryhua.nanjin.kafka.streaming.KafkaStreamsAbnormallyStopped
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka.*
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions.*
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes.*
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)
  case class TableTwo(name: String, color: Int)

  case class StreamTarget(name: String, weight: Int, color: Int)

  val s1Topic: KafkaTopic[IO, Int, StreamOne] = ctx.topic[Int, StreamOne]("stream.test.join.stream.one")
  val t2Topic: KafkaTopic[IO, Int, TableTwo]  = ctx.topic[Int, TableTwo]("stream.test.join.table.two")

  val tgt: KafkaTopic[IO, Int, StreamTarget] = ctx.topic[Int, StreamTarget]("stream.test.join.target")

  val sendT2Data: Stream[IO, ProducerResult[Int, TableTwo]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(t2Topic.topicName, 1, TableTwo("x", 0)),
        ProducerRecord(t2Topic.topicName, 2, TableTwo("y", 1)),
        ProducerRecord(t2Topic.topicName, 3, TableTwo("z", 2))
      ))).covary[IO].through(t2Topic.fs2Channel.producerPipe)

  val harvest: Stream[IO, StreamTarget] =
    tgt.fs2Channel.stream
      .map(x => tgt.decoder(x).decode)
      .observe(_.map(_.offset).through(commitBatchWithin[IO](1, 1.seconds)).drain)
      .map(_.record.value)
      .debug()

  val expected: Set[StreamTarget] = Set(StreamTarget("a", 0, 0), StreamTarget("b", 0, 1), StreamTarget("c", 0, 2))
}

class KafkaStreamingTest extends AnyFunSuite with BeforeAndAfter {
  import KafkaStreamingData.*

  implicit val oneValue: Serde[StreamOne] = s1Topic.codec.valSerde
  implicit val twoValue: Serde[TableTwo]  = t2Topic.codec.valSerde

  before(sendT2Data.compile.drain.unsafeRunSync())

  test("stream-table join") {
    val sendS1Data: Stream[IO, ProducerResult[Int, StreamOne]] = Stream
      .emits(List(
        s1Topic.fs2ProducerRecord(101, StreamOne("na", -1)),
        s1Topic.fs2ProducerRecord(102, StreamOne("na", -1)),
        s1Topic.fs2ProducerRecord(103, StreamOne("na", -1)),
        s1Topic.fs2ProducerRecord(1, StreamOne("a", 0)),
        s1Topic.fs2ProducerRecord(2, StreamOne("b", 1)),
        s1Topic.fs2ProducerRecord(3, StreamOne("c", 2)),
        s1Topic.fs2ProducerRecord(201, StreamOne("d", 3)),
        s1Topic.fs2ProducerRecord(202, StreamOne("e", 4))
      ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(s1Topic.fs2Channel.producerPipe)

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a
      .join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(s"out=($k, $v)"))
      .to(tgt.topicName)(tgt.asProduced)

    val res: Set[StreamTarget] = ctx
      .buildStreams(top)
      .query
      .concurrently(sendS1Data)
      .flatMap(_ => harvest.interruptAfter(10.seconds))
      .compile
      .toList
      .unsafeRunSync()
      .toSet
    assert(res == expected)
  }

  test("kafka stream has bad records") {
    val tn         = TopicName("stream.test.stream.badrecords.one")
    val s1Topic    = ctx.topic[Int, StreamOne](tn)
    val s1TopicBin = ctx.topic[Int, Array[Byte]](tn)

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1TopicBin.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a
      .flatMapValues(v => s1Topic.codec.valCodec.tryDecode(v).toOption)
      .join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(s"out=($k, $v)"))
      .to(tgt.topicName)(tgt.asProduced)

    val sendS1Data: Stream[IO, ProducerResult[Int, Array[Byte]]] = Stream
      .emits(List(
        s1TopicBin.fs2ProducerRecord(1, s1Topic.serializeVal(StreamOne("a", 1))),
        s1TopicBin.fs2ProducerRecord(2, "exception1".getBytes),
        s1TopicBin.fs2ProducerRecord(3, s1Topic.serializeVal(StreamOne("c", 3))),
        s1TopicBin.fs2ProducerRecord(4, s1Topic.serializeVal(StreamOne("d", 4))),
        s1TopicBin.fs2ProducerRecord(5, "exception2".getBytes),
        s1TopicBin.fs2ProducerRecord(6, s1Topic.serializeVal(StreamOne("f", 6)))
      ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(s1TopicBin.fs2Channel.producerPipe)
      .debug()

    val res = ctx
      .buildStreams(top)
      .query
      .concurrently(sendS1Data)
      .flatMap(_ => harvest.interruptAfter(10.seconds))
      .compile
      .toList
      .unsafeRunSync()
    assert(res == List(StreamTarget("a", 0, 0), StreamTarget("c", 0, 2)))
  }

  test("kafka stream exception") {
    val tn         = TopicName("stream.test.stream.exception.one")
    val s1Topic    = ctx.topic[Int, StreamOne](tn)
    val s1TopicBin = ctx.topic[Int, Array[Byte]](tn)

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a
      .join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(s"out=($k, $v)"))
      .to(tgt.topicName)(tgt.asProduced)

    val sendS1Data: Stream[IO, ProducerResult[Int, Array[Byte]]] = Stream
      .emits(List(
        s1TopicBin.fs2ProducerRecord(1, s1Topic.serializeVal(StreamOne("a", 1))),
        s1TopicBin.fs2ProducerRecord(2, "exception1".getBytes),
        s1TopicBin.fs2ProducerRecord(3, s1Topic.serializeVal(StreamOne("c", 3)))
      ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(s1TopicBin.fs2Channel.producerPipe)
      .debug()

    assertThrows[KafkaStreamsAbnormallyStopped.type](
      ctx
        .buildStreams(top)
        .query
        .concurrently(sendS1Data)
        .flatMap(_ => harvest.interruptAfter(10.seconds))
        .compile
        .toList
        .unsafeRunSync())
  }
}
