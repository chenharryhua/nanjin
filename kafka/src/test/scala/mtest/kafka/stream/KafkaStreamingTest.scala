package mtest.kafka.stream

import cats.Id
import cats.data.{Kleisli, Reader}
import cats.derived.auto.show.*
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.global
import cats.implicits.{catsSyntaxTuple2Semigroupal, showInterpolator}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.KafkaTopic
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords}
import io.circe.syntax.EncoderOps
import mtest.kafka.*
import org.apache.kafka.common.serialization.Serde
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

  val s1Topic: KafkaTopic[IO, Int, StreamOne] = ctx.topic[Int, StreamOne]("stream.test.join.stream.one")
  val t2Topic: KafkaTopic[IO, Int, TableTwo]  = ctx.topic[Int, TableTwo]("stream.test.join.table.two")

  val tgt: KafkaTopic[IO, Int, StreamTarget] = ctx.topic[Int, StreamTarget]("stream.test.join.target")

  val sendT2Data =
    Stream(
      ProducerRecords(List(
        ProducerRecord(t2Topic.topicName.value, 1, TableTwo("x", 0)),
        ProducerRecord(t2Topic.topicName.value, 2, TableTwo("y", 1)),
        ProducerRecord(t2Topic.topicName.value, 3, TableTwo("z", 2))
      ))).covary[IO].through(ctx.producer[Int, TableTwo].sink)

  val harvest: Stream[IO, StreamTarget] =
    ctx
      .consume(tgt.topicName)
      .stream
      .map(x => tgt.serde.deserialize(x))
      .observe(_.map(_.offset).through(commitBatchWithin[IO](1, 0.1.seconds)).drain)
      .map(_.record.value)
      .debug(o => show"harvest: $o")
      .onFinalize(IO.sleep(1.seconds))

  val expected: Set[StreamTarget] =
    Set(StreamTarget("a", 0, 0), StreamTarget("b", 0, 1), StreamTarget("c", 0, 2))
}
@DoNotDiscover
class KafkaStreamingTest extends AnyFunSuite with BeforeAndAfter {
  import KafkaStreamingData.*

  implicit val oneValue: Serde[StreamOne] = s1Topic.serdePair.value.serde
  implicit val twoValue: Serde[TableTwo]  = t2Topic.serdePair.value.serde

  val appId = "kafka_stream_test"

  before(sendT2Data.compile.drain.unsafeRunSync())

  test("stream-table join") {
    val sendS1Data = Stream
      .emits(
        List(
          s1Topic.topicDef.producerRecord(101, StreamOne("na", -1)),
          s1Topic.topicDef.producerRecord(102, StreamOne("na", -1)),
          s1Topic.topicDef.producerRecord(103, StreamOne("na", -1)),
          s1Topic.topicDef.producerRecord(1, StreamOne("a", 0)),
          s1Topic.topicDef.producerRecord(2, StreamOne("b", 1)),
          s1Topic.topicDef.producerRecord(3, StreamOne("c", 2)),
          s1Topic.topicDef.producerRecord(201, StreamOne("d", 3)),
          s1Topic.topicDef.producerRecord(202, StreamOne("e", 4))
        ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(ctx.producer[Int, StreamOne].sink)

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1Topic.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a
      .join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to(tgt.topicName.value)(tgt.asProduced)

    val res: Set[StreamTarget] = (IO.println(Console.CYAN + "stream-table join" + Console.RESET) >> ctx
      .buildStreams(appId, top)
      .kafkaStreams
      .concurrently(sendS1Data)
      .flatMap(_ => harvest.interruptAfter(10.seconds))
      .compile
      .toList).unsafeRunSync().toSet
    assert(res == expected)
  }

  test("kafka stream has bad records") {
    val tn         = TopicName("stream.test.stream.badrecords.one")
    val s1Topic    = ctx.topic[Int, StreamOne](tn)
    val s1TopicBin = ctx.topic[Array[Byte], Array[Byte]](tn)

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      a <- s1TopicBin.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a.flatMap { (k, v) =>
      (s1Topic.serdePair.key.tryDeserialize(k).toOption, s1Topic.serdePair.value.tryDeserialize(v).toOption)
        .mapN((_, _))
    }.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to(tgt.topicName.value)(tgt.asProduced)

    val sendS1Data = Stream
      .emits(
        List(
          s1TopicBin.topicDef
            .producerRecord(s1Topic.serde.serializeKey(1), s1Topic.serde.serializeVal(StreamOne("a", 1))),
          s1TopicBin.topicDef.producerRecord(s1Topic.serde.serializeKey(2), "exception1".getBytes),
          s1TopicBin.topicDef
            .producerRecord(s1Topic.serde.serializeKey(3), s1Topic.serde.serializeVal(StreamOne("c", 3))),
          s1TopicBin.topicDef
            .producerRecord(s1Topic.serde.serializeKey(4), s1Topic.serde.serializeVal(StreamOne("d", 4))),
          s1TopicBin.topicDef.producerRecord(s1Topic.serde.serializeKey(5), "exception2".getBytes),
          s1TopicBin.topicDef
            .producerRecord(s1Topic.serde.serializeKey(6), s1Topic.serde.serializeVal(StreamOne("f", 6)))
        ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(ctx.producer[Array[Byte], Array[Byte]].sink)
      .debug()

    val res = (IO.println(Console.CYAN + "kafka stream has bad records" + Console.RESET) >> ctx
      .buildStreams(appId, top)
      .kafkaStreams
      .concurrently(sendS1Data)
      .flatMap(_ => harvest.interruptAfter(10.seconds))
      .compile
      .toList).unsafeRunSync()
    assert(res.distinct == List(StreamTarget("a", 0, 0), StreamTarget("c", 0, 2)))
  }

  test("kafka stream exception") {
    val tn         = TopicName("stream.test.stream.exception.one")
    val s1Topic    = ctx.topic[Int, StreamOne](tn)
    val s1TopicBin = ctx.topic[Int, Array[Byte]](tn)

    val top: Reader[StreamsBuilder, Unit] = for {
      a <- s1Topic.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a
      .join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to(tgt.topicName.value)(tgt.asProduced)

    val sendS1Data = Stream
      .emits(List(
        s1TopicBin.topicDef.producerRecord(1, s1Topic.serde.serializeVal(StreamOne("a", 1))),
        s1TopicBin.topicDef.producerRecord(2, "exception1".getBytes),
        s1TopicBin.topicDef.producerRecord(3, s1Topic.serde.serializeVal(StreamOne("c", 3)))
      ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(ctx.producer[Int, Array[Byte]].sink)
      .debug()

    assertThrows[Exception](
      (IO.println(Console.CYAN + "kafka stream exception" + Console.RESET) >> ctx
        .buildStreams(appId, top)
        .stateUpdates
        .map(_.asJson)
        .debug()
        .concurrently(sendS1Data)
        .concurrently(harvest)
        .interruptAfter(1.day)
        .compile
        .drain).unsafeRunSync())
  }

  test("kafka stream should be able to be closed") {
    val s1Topic = ctx.topic[Int, StreamOne]("stream.test.join.stream.one")

    val top: Reader[StreamsBuilder, Unit] = for {
      a <- s1Topic.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color)).to(tgt.topicName.value)(tgt.asProduced)

    (IO.println(Console.CYAN + "kafka stream should be able to be closed" + Console.RESET) >> ctx
      .buildStreams(appId, top)
      .kafkaStreams
      .flatMap(ks =>
        Stream
          .fixedRate[IO](1.seconds)
          .evalTap(_ => IO.println("running..."))
          .concurrently(Stream.sleep[IO](5.second).map(_ => ks.close())))
      .compile
      .drain
      .guaranteeCase {
        case Outcome.Succeeded(_) => IO(assert(true)).void
        case Outcome.Errored(_)   => IO(assert(false)).void
        case Outcome.Canceled()   => IO(assert(false)).void
      }).unsafeRunSync()
  }

  test("should raise an error when kafka topic does not exist") {
    val s1Topic = ctx.topic[Int, StreamOne]("consumer.topic.does.not.exist")

    val top: Reader[StreamsBuilder, Unit] = for {
      a <- s1Topic.asConsumer.kstream
      b <- t2Topic.asConsumer.ktable
    } yield a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color)).to(tgt.topicName.value)(tgt.asProduced)

    val res = IO.println(Console.CYAN + "kafka topic does not exist" + Console.RESET) >> ctx
      .buildStreams(appId, top)
      .stateUpdates
      .debug()
      .compile
      .drain
      .guaranteeCase {
        case Outcome.Succeeded(_) => IO(assert(false)).void
        case Outcome.Errored(_)   => IO(assert(true)).void
        case Outcome.Canceled()   => IO(assert(false)).void
      }
    assertThrows[Throwable](res.unsafeRunSync())
  }
}
