package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.streaming.KafkaStreamsBuilder
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka.*
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier, Record}
import org.apache.kafka.streams.scala.ImplicitConversions.*
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes.*
import org.apache.kafka.streams.state.KeyValueStore
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
@DoNotDiscover
class TransformerTest extends AnyFunSuite {
  val appid = "transform_test"

  test("stream transformer") {
    val store = ctx.store[Int, String]("stream.builder.test.store")

    def td = TopicDef[Int, String](TopicName("stream"))

    val topic1 = ctx.topic[Int, String](td.withTopicName("stream.builder.test.stream1"))
    val topic2 = ctx.topic[Int, String](td.withTopicName("stream.builder.test.table2"))
    val tgt    = ctx.topic[Int, String](td.withTopicName("stream.builder.test.target"))

    val processor: ProcessorSupplier[Int, String, Int, String] =
      new ProcessorSupplier[Int, String, Int, String] {
        var kvStore: KeyValueStore[Int, String]                 = _
        var ctx: api.ProcessorContext[Int, String]              = _
        override def get(): Processor[Int, String, Int, String] = new Processor[Int, String, Int, String] {
          override def init(context: api.ProcessorContext[Int, String]): Unit = {
            kvStore = context.getStateStore[KeyValueStore[Int, String]](store.name)
            ctx = context
            println("transformer initialized")
          }

          override def close(): Unit =
            // kvStore.close()
            println("transformer closed")

          override def process(record: Record[Int, String]): Unit = {
            println(record.toString)
            kvStore.put(record.key(), record.value())
            ctx.forward(record)
          }
        }
      }

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      s1 <- topic1.asConsumer.kstream
      t2 <- topic2.asConsumer.ktable
    } yield s1.process(processor, store.name).join(t2)(_ + _).to(tgt.topicName.value)(tgt.asProduced)

    val kafkaStreamService: KafkaStreamsBuilder[IO] =
      ctx.buildStreams(appid, top).addStateStore(store.inMemoryKeyValueStore.keyValueStoreBuilder)
    println(kafkaStreamService.topology.describe())

    val t2Data = Stream(
      ProducerRecords(
        List(
          ProducerRecord(topic2.topicName.value, 2, "t0"),
          ProducerRecord(topic2.topicName.value, 4, "t1"),
          ProducerRecord(topic2.topicName.value, 6, "t2"))))
      .covary[IO]
      .through(ctx.producer(td.rawSerdes).sink)

    val s1Data: Stream[IO, ProducerResult[Int, String]] =
      Stream
        .awakeEvery[IO](1.seconds)
        .zipWithIndex
        .map { case (_, index) =>
          ProducerRecords.one(ProducerRecord(topic1.topicName.value, index.toInt, s"stream$index"))
        }
        .through(ctx.producer[Int, String].sink)
    val havest = ctx
      .consumer(tgt.topicName)
      .stream
      .map(tgt.serde.deserialize(_))
      .debug()
      .observe(_.map(_.offset).through(commitBatchWithin(10, 2.seconds)).drain)

    val res =
      havest
        .concurrently(kafkaStreamService.stateUpdates)
        .concurrently(t2Data)
        .concurrently(s1Data)
        .interruptAfter(15.seconds)
        .compile
        .toList
        .unsafeRunSync()

    println(Console.CYAN + "stream transformer" + Console.RESET)
    assert(res.map(_.record.key).toSet == Set(2, 4, 6))
  }
}
