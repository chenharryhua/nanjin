package mtest.kafka.stream

import cats.data.Reader
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.kafka.KafkaStreamsStoppedException
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import mtest.kafka.*
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.serialization.Serdes.*
import org.apache.kafka.streams.state.{QueryableStoreTypes, Stores}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters.*
import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.util.Random

@DoNotDiscover
class InteractiveTest extends AnyFunSuite {
  val topic      = ctx.topic[Int, String]("stream.test.interactive.2")
  val storeName  = "stream.test.interactive.local.store.2"
  val gstoreName = "stream.test.interactive.store.global.2"

  val mat  = Materialized.as[Int, String](Stores.inMemoryKeyValueStore(storeName))
  val gmat = Materialized.as[Int, String](Stores.persistentKeyValueStore(gstoreName))

  val top: Reader[StreamsBuilder, Unit]  = topic.kafkaStream.ktable(mat).void
  val gtop: Reader[StreamsBuilder, Unit] = topic.kafkaStream.gktable(gmat).void

  test("interactive") {

    val sq  = StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore[Int, String]())
    val gsq = StoreQueryParameters.fromNameAndType(gstoreName, QueryableStoreTypes.keyValueStore[Int, String]())

    val data =
      Stream(ProducerRecords.one(ProducerRecord(topic.topicName.value, Random.nextInt(3), s"a${Random.nextInt(1000)}")))
        .covary[IO]
        .through(topic.fs2Channel.producerPipe)

    val res =
      for {
        _ <- data
        kss1 <- ctx.buildStreams(top).query
        kss2 <- ctx.buildStreams(gtop).query
      } yield {
        val g = kss1.store(sq).all().asScala.toList.sortBy(_.key)
        val q = kss2.store(gsq).all().asScala.toList.sortBy(_.key)
        assert(q == g)
        q
      }
    println(ctx.buildStreams(top).topology.describe())
    println(res.compile.toList.unsafeRunSync().flatten)
  }

  test("startup timeout") {
    val to1 = ctx.buildStreams(top).withStartUpTimeout(0.seconds).stream.compile.drain
    assertThrows[TimeoutException](to1.unsafeRunSync())
    val to2 = ctx.buildStreams(top).withStartUpTimeout(100.seconds).query.compile.drain
    to2.unsafeRunSync()
  }

  test("detect stream stop") {
    val to1 =
      ctx.buildStreams(top).query.evalMap(ks => IO.sleep(1.seconds) >> IO(ks.close())) >> Stream.sleep[IO](1.hour)
    assertThrows[KafkaStreamsStoppedException](to1.compile.drain.unsafeRunSync())
  }
}
