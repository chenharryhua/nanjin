package mtest.kafka.stream

import cats.data.Reader
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import mtest.kafka.*
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random

class InteractiveTest extends AnyFunSuite {
  val topic       = ctx.topic[Int, String]("stream.test.interactive.2")
  val localStore  = topic.asStateStore("stream.test.interactive.local.store.2")
  val globalStore = topic.asStateStore("stream.test.interactive.store.global.2")

  val top: Reader[StreamsBuilder, Unit] =
    topic.asConsumer.withProcessorName("abc").ktable(localStore.inMemoryKeyValueStore.supplier).void
  val gtop: Reader[StreamsBuilder, Unit] =
    topic.asConsumer.gktable(globalStore.persistentKeyValueStore.supplier).void

  test("interactive") {

    val data =
      Stream(ProducerRecords.one(ProducerRecord(topic.topicName.value, Random.nextInt(3), s"a${Random.nextInt(1000)}")))
        .covary[IO]
        .through(topic.fs2Channel.producerPipe)

    val res: Stream[IO, List[KeyValue[Int, String]]] =
      for {
        _ <- data
        kss1 <- ctx.buildStreams(top).kafkaStreams
        kss2 <- ctx.buildStreams(gtop).kafkaStreams
      } yield {
        val g = kss1.store(localStore.query.keyValueStore).all().asScala.toList.sortBy(_.key)
        val q = kss2.store(globalStore.query.keyValueStore).all().asScala.toList.sortBy(_.key)
        assert(q == g)
        q
      }
    println(ctx.buildStreams(top).topology.describe())
    println(res.compile.toList.unsafeRunSync().flatten)
  }

  test("startup timeout") {
    val to1 = ctx.buildStreams(top).withStartUpTimeout(0.seconds).stateStream.compile.drain
    assertThrows[TimeoutException](to1.unsafeRunSync())
    val to2 = ctx.buildStreams(top).withStartUpTimeout(1.day).kafkaStreams.map(_.state()).debug().compile.drain
    to2.unsafeRunSync()
  }

  test("detect stream stop") {
    val to1 =
      ctx.buildStreams(top).kafkaStreams.evalMap(ks => IO.sleep(1.seconds) >> IO(ks.close()) >> IO.sleep(1.day))
    to1.compile.drain.unsafeRunSync()
  }

  test("detect stream error") {
    val to1 =
      ctx.buildStreams(top).kafkaStreams.evalMap(ks => IO.sleep(1.seconds) >> IO(ks.cleanUp()) >> IO.sleep(1.day))
    assertThrows[IllegalStateException](to1.compile.drain.unsafeRunSync())
  }
}
