package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka.*
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.errors.StreamsException
import org.apache.kafka.streams.scala.ImplicitConversions.*
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, DoNotDiscover}

import scala.concurrent.duration.*
object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)
  case class TableTwo(name: String, color: Int)

  case class StreamTarget(name: String, weight: Int, color: Int)

  val s1Topic: KafkaTopic[IO, Int, StreamOne] = ctx.topic[Int, StreamOne]("stream.test.stream.one")
  val t2Topic: KafkaTopic[IO, Int, TableTwo]  = ctx.topic[Int, TableTwo]("stream.test.table.two")

  val tgt: KafkaTopic[IO, Int, StreamTarget] = ctx.topic[Int, StreamTarget]("stream.test.join.target")

  val s1Data: Stream[IO, ProducerRecords[Unit, Int, StreamOne]] = Stream
    .emits(
      List(
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 101, StreamOne("na", -1)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 102, StreamOne("na", -1)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 103, StreamOne("na", -1)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 1, StreamOne("a", 0)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 2, StreamOne("b", 1)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 3, StreamOne("c", 2)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 201, StreamOne("d", 3)),
        ProducerRecord[Int, StreamOne](s1Topic.topicName.value, 202, StreamOne("e", 4))
      ).map(ProducerRecords.one(_)))
    .covary[IO]

  val sendS1Data: Stream[IO, ProducerResult[Unit, Int, StreamOne]] =
    Stream.fixedRate[IO](1.seconds).zipRight(s1Data).through(s1Topic.fs2Channel.producerPipe).debug()

  val sendT2Data: Stream[IO, ProducerResult[Unit, Int, TableTwo]] =
    Stream(
      ProducerRecords(List(
        ProducerRecord(t2Topic.topicName.value, 1, TableTwo("x", 0)),
        ProducerRecord(t2Topic.topicName.value, 2, TableTwo("y", 1)),
        ProducerRecord(t2Topic.topicName.value, 3, TableTwo("z", 2))
      ))).covary[IO].through(t2Topic.fs2Channel.producerPipe)

  val expected: Set[StreamTarget] = Set(StreamTarget("a", 0, 0), StreamTarget("b", 0, 1), StreamTarget("c", 0, 2))
}

@DoNotDiscover
class KafkaStreamingTest extends AnyFunSuite with BeforeAndAfter {
  import KafkaStreamingData.*

  implicit val oneValue: Serde[StreamOne]    = s1Topic.registered.valSerde
  implicit val twoValue: Serde[TableTwo]     = t2Topic.registered.valSerde
  implicit val tgtValue: Serde[StreamTarget] = tgt.registered.valSerde

  before(sendT2Data.compile.drain.unsafeRunSync())

  test("stream-table join") {
    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.kafkaStream.kstream
      b <- t2Topic.kafkaStream.ktable
    } yield a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color)).to(tgt)

    val harvest: Stream[IO, StreamTarget] =
      tgt.fs2Channel.stream
        .map(x => tgt.decoder(x).decode)
        .observe(_.map(_.offset).through(commitBatchWithin[IO](1, 1.seconds)).drain)
        .map(_.record.value)
        .debug()
    val res: Set[StreamTarget] =
      harvest
        .concurrently(sendS1Data)
        .concurrently(ctx.buildStreams(top).stream.debug().delayBy(2.seconds))
        .interruptAfter(15.seconds)
        .compile
        .toList
        .unsafeRunSync()
        .toSet
    assert(res == expected)
  }

  test("kafka stream throw exception") {
    val s1Topic: KafkaTopic[IO, Int, StreamOne]      = ctx.topic[Int, StreamOne]("stream.test.stream.exception.one")
    val s1TopicBin: KafkaTopic[IO, Int, Array[Byte]] = ctx.topic[Int, Array[Byte]]("stream.test.stream.exception.one")

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.kafkaStream.kstream
      b <- t2Topic.kafkaStream.ktable
    } yield a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color)).to(tgt)

    val harvest: Stream[IO, StreamTarget] =
      tgt.fs2Channel.stream
        .map(x => tgt.decoder(x).decode)
        .observe(_.map(_.offset).through(commitBatchWithin[IO](1, 1.seconds)).drain)
        .map(_.record.value)
        .debug()
    val s1Data: Stream[IO, ProducerRecords[Unit, Int, Array[Byte]]] = Stream
      .emits(
        List(
          ProducerRecord[Int, Array[Byte]](
            s1Topic.topicName.value,
            101,
            oneValue.serializer().serialize(s1Topic.topicName.value, StreamOne("na", -1))),
          ProducerRecord[Int, Array[Byte]](
            s1Topic.topicName.value,
            102,
            oneValue.serializer().serialize(s1Topic.topicName.value, StreamOne("na", -1))),
          ProducerRecord[Int, Array[Byte]](
            s1Topic.topicName.value,
            103,
            oneValue.serializer().serialize(s1Topic.topicName.value, StreamOne("na", -1))),
          ProducerRecord[Int, Array[Byte]](
            s1Topic.topicName.value,
            104,
            oneValue.serializer().serialize(s1Topic.topicName.value, StreamOne("na", -1))),
          ProducerRecord[Int, Array[Byte]](s1Topic.topicName.value, 105, "exception".getBytes),
          ProducerRecord[Int, Array[Byte]](
            s1Topic.topicName.value,
            106,
            oneValue.serializer().serialize(s1Topic.topicName.value, StreamOne("na", -1)))
        ).map(ProducerRecords.one(_)))
      .covary[IO]

    val sendS1Data =
      Stream.fixedRate[IO](1.seconds).zipRight(s1Data).through(s1TopicBin.fs2Channel.producerPipe).debug()

    val res = s1Topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence >> IO.sleep(1.seconds) >>
      harvest
        .concurrently(sendS1Data)
        .concurrently(ctx.buildStreams(top).stream.debug().delayBy(1.seconds))
        .compile
        .toList

    assertThrows[StreamsException](res.unsafeRunSync())
  }
}
