package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.StoreName
import fs2.Stream
import fs2.kafka.{commitBatchWithin, ProducerRecord, ProducerRecords}
import mtest.kafka._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

@DoNotDiscover
class TransformerTest extends AnyFunSuite {

  test("stream transformer") {
    val storeName = StoreName("stream.builder.test.store")
    val store: StoreBuilder[KeyValueStore[Int, String]] =
      Stores.keyValueStoreBuilder(Stores.inMemoryKeyValueStore(storeName.value), ctx.asKey[Int], ctx.asValue[String])

    val topic1 = ctx.topic[Int, String]("stream.builder.test.stream1")
    val topic2 = ctx.topic[Int, String]("stream.builder.test.table2")
    val tgt    = ctx.topic[Int, String]("stream.builder.test.target")

    val transformer: TransformerSupplier[Int, String, KeyValue[Int, String]] =
      () =>
        new Transformer[Int, String, KeyValue[Int, String]] {
          var kvStore: KeyValueStore[Int, String] = _

          override def init(processorContext: ProcessorContext): Unit = {
            kvStore = processorContext.getStateStore[KeyValueStore[Int, String]](storeName.value)
            println("transformer initialized")
          }

          override def transform(k: Int, v: String): KeyValue[Int, String] = {
            kvStore.put(k, v)
            println((k, v))
            new KeyValue[Int, String](k, v + "-local-transformed-")
          }

          override def close(): Unit =
            println("transformer closed")
        }

    val top: Kleisli[Id, StreamsBuilder, Unit] = for {
      s1 <- topic1.kafkaStream.kstream
      t2 <- topic2.kafkaStream.ktable
    } yield s1.transform(transformer, storeName.value).join(t2)(_ + _).to(tgt)

    val kafkaStreamService = ctx.buildStreams(top).addStateStore(store).withProperty("unknown-feature", "unused")
    println(kafkaStreamService.topology.describe())

    val t2Data = Stream(
      ProducerRecords(
        List(
          ProducerRecord(topic2.topicName.value, 2, "t0"),
          ProducerRecord(topic2.topicName.value, 4, "t1"),
          ProducerRecord(topic2.topicName.value, 6, "t2")))).covary[IO].through(topic2.fs2Channel.producerPipe)

    val s1Data =
      Stream
        .awakeEvery[IO](1.seconds)
        .zipWithIndex
        .map { case (_, index) =>
          ProducerRecords.one(ProducerRecord(topic1.topicName.value, index.toInt, s"stream$index"))
        }
        .through(topic1.fs2Channel.producerPipe)
    val havest = tgt.fs2Channel.stream
      .map(tgt.decoder(_).decode)
      .debug()
      .observe(_.map(_.offset).through(commitBatchWithin(10, 2.seconds)).drain)

    val res =
      (havest
        .concurrently(kafkaStreamService.stream)
        .concurrently(t2Data)
        .concurrently(s1Data)
        .interruptAfter(15.seconds)
        .compile
        .toList)
        .unsafeRunSync()
    assert(res.map(_.record.key).toSet == Set(2, 4, 6))
  }
}
