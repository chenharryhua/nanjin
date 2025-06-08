package mtest.kafka.stream

import cats.derived.auto.show.*
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.global
import cats.implicits.showInterpolator
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords}
import mtest.kafka.*
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, DoNotDiscover}

import scala.concurrent.duration.*

object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)
  case class TableTwo(name: String, color: Int)

  case class StreamTarget(name: String, weight: Int, color: Int)

  val s1Def: TopicDef[Int, StreamOne] = TopicDef[Int, StreamOne](TopicName("stream.test.join.stream.one"))

  val s1Topic = s1Def
  val t2Topic =
    TopicDef[Int, TableTwo](TopicName("stream.test.join.table.two"))

  val tgt = TopicDef[Int, StreamTarget](TopicName("stream.test.join.target"))
  val serde = ctx.serde(tgt)

  val sendT2Data =
    Stream(
      ProducerRecords(List(
        ProducerRecord(t2Topic.topicName.value, 1, TableTwo("x", 0)),
        ProducerRecord(t2Topic.topicName.value, 2, TableTwo("y", 1)),
        ProducerRecord(t2Topic.topicName.value, 3, TableTwo("z", 2))
      ))).covary[IO].through(ctx.produce(t2Topic.codecPair).sink)

  val harvest: Stream[IO, StreamTarget] =
    ctx
      .consume(tgt.topicName)
      .stream
      .map(x => serde.deserialize(x))
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

  val appId = "kafka_stream_test"

  before(sendT2Data.compile.drain.unsafeRunSync())

  test("stream-table join") {
    val sendS1Data = Stream
      .emits(
        List(
          s1Topic.producerRecord(101, StreamOne("na", -1)),
          s1Topic.producerRecord(102, StreamOne("na", -1)),
          s1Topic.producerRecord(103, StreamOne("na", -1)),
          s1Topic.producerRecord(1, StreamOne("a", 0)),
          s1Topic.producerRecord(2, StreamOne("b", 1)),
          s1Topic.producerRecord(3, StreamOne("c", 2)),
          s1Topic.producerRecord(201, StreamOne("d", 3)),
          s1Topic.producerRecord(202, StreamOne("e", 4))
        ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(ctx.produce[Int, StreamOne].sink)

    val res: Set[StreamTarget] = (IO.println(Console.CYAN + "stream-table join" + Console.RESET) >> ctx
      .buildStreams(appId)(apps.kafka_streaming)
      .kafkaStreams
      .concurrently(sendS1Data)
      .flatMap(_ => harvest.interruptAfter(10.seconds))
      .compile
      .toList).unsafeRunSync().toSet
    assert(res == expected)
  }

  test("kafka stream should be able to be closed") {

    (IO.println(Console.CYAN + "kafka stream should be able to be closed" + Console.RESET) >> ctx
      .buildStreams("app-close")(apps.kafka_streaming)
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
}
