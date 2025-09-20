package mtest.kafka.stream
import cats.derived.auto.show.*
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.unsafe.implicits.global
import cats.implicits.showInterpolator
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.streaming.StreamsSerde
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, KafkaGenericSerde}
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{commitBatchWithin, AutoOffsetReset, ProducerRecords, ProducerResult}
import mtest.kafka.*
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfter, DoNotDiscover}

import scala.concurrent.duration.*

object KafkaStreamingData {

  case class StreamOne(name: String, size: Int)
  case class TableTwo(name: String, color: Int)

  case class StreamTarget(name: String, weight: Int, color: Int)

  val s1Def: AvroTopic[Int, StreamOne] = AvroTopic[Int, StreamOne](TopicName("stream.test.join.stream.one"))

  val s1Topic: AvroTopic[Int, StreamOne] = s1Def
  val t2Topic: AvroTopic[Int, TableTwo] =
    AvroTopic[Int, TableTwo](TopicName("stream.test.join.table.two"))

  val tgt = AvroTopic[Int, StreamTarget](TopicName("stream.test.join.target"))
  val serde: KafkaGenericSerde[Int, StreamTarget] = ctx.serde(tgt)

  val sendT2Data: IO[ProducerResult[Int, TableTwo]] =
    ctx
      .produce(t2Topic)
      .produce(
        List(
          1 -> TableTwo("x", 0),
          2 -> TableTwo("y", 1),
          3 -> TableTwo("z", 2)
        )
      )

  val harvest: Stream[IO, StreamTarget] =
    ctx
      .consume(tgt)
      .updateConfig(_.withGroupId("harvest").withAutoOffsetReset(AutoOffsetReset.Earliest))
      .subscribe
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

  before(sendT2Data.unsafeRunSync())

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
      .through(ctx.sharedProduce[Int, StreamOne](s1Def.pair).sink)

    val res: Set[StreamTarget] = (IO.println(Console.CYAN + "stream-table join" + Console.RESET) >> ctx
      .buildStreams(appId)(apps.kafka_streaming)
      .withProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)
      .kafkaStreams
      .concurrently(sendS1Data)
      .flatMap(_ => harvest.interruptAfter(10.seconds))
      .compile
      .toList).unsafeRunSync().toSet
    println(
      ctx
        .buildStreams(appId)(apps.kafka_streaming)
        .withProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE)
        .topology
        .describe())
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

  test("kafka stream exception") {
    val tn = TopicName("stream.test.stream.exception.one")
    val s1Topic: AvroTopic[Int, StreamOne] = s1Def.withTopicName(tn.name)
    val s1TopicBin: AvroTopic[Int, Array[Byte]] = AvroTopic[Int, Array[Byte]](tn)

    def top(sb: StreamsBuilder, ss: StreamsSerde): Unit = {
      import ss.implicits.*

      val a = sb.stream[Int, StreamOne](s1Topic.topicName.value)
      val b = sb.table[Int, TableTwo](t2Topic.topicName.value)

      a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
        .peek((k, v) => println(show"out=($k, $v)"))
        .to(tgt.topicName.value)
    }
    val serde = ctx.serde(s1Topic)
    val sendS1Data = Stream
      .emits(List(
        s1TopicBin.producerRecord(1, serde.serializeVal(StreamOne("a", 1))),
        s1TopicBin.producerRecord(2, "exception1".getBytes),
        s1TopicBin.producerRecord(3, serde.serializeVal(StreamOne("c", 3)))
      ).map(ProducerRecords.one))
      .covary[IO]
      .metered(1.seconds)
      .through(ctx.sharedProduce[Int, Array[Byte]](s1TopicBin.pair).sink)
      .debug()

    assertThrows[Exception](
      (IO.println(Console.CYAN + "kafka stream exception" + Console.RESET) >> ctx
        .buildStreams(appId)(top)
        .stateUpdates
        .debug()
        .concurrently(sendS1Data)
        .concurrently(harvest)
        .interruptAfter(1.day)
        .compile
        .drain).unsafeRunSync())
  }

}
