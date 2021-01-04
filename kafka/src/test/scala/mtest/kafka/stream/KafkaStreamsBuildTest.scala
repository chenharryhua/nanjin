package mtest.kafka.stream

import cats.Id
import cats.data.Kleisli
import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.kafka.StoreName
import fs2.Stream
import mtest.kafka._
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{Transformer, TransformerSupplier}
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

@DoNotDiscover
class KafkaStreamsBuildTest extends AnyFunSuite {

  test("stream builder") {
    val storeName = StoreName("stream.test.store")
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
            kvStore = processorContext.getStateStore(storeName.value).asInstanceOf[KeyValueStore[Int, String]]
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

    val prepare = for {
      _ <- topic1.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic1.admin.newTopic(1, 1).attempt
      _ <- topic2.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic2.admin.newTopic(1, 1).attempt
      _ <- tgt.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- tgt.admin.newTopic(1, 1).attempt
    } yield ()

    val sender = for {
      _ <- Stream.eval(topic2.send(List(topic2.fs2PR(0, "t0"), topic2.fs2PR(1, "t1"), topic2.fs2PR(2, "t2"))))
      resp <- Stream.awakeEvery[IO](1.seconds).zipWithIndex.evalMap { case (_, index) =>
        topic1.send(index.toInt, s"stream$index")
      }
    } yield println(resp)

    val havest = tgt.fs2Channel.stream.map(tgt.decoder(_).decode)

    val res =
      (prepare >> IO.sleep(1.second) >> kafkaStreamService.run
        .flatMap(_ => havest)
        .concurrently(sender)
        .interruptAfter(8.seconds)
        .compile
        .toList).unsafeRunSync()
    println(res)
    assert(res.map(_.record.key).toSet == Set(0, 1, 2))
  }
} 
