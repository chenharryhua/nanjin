package mtest.kafka.stream

import cats.data.Reader
import cats.syntax.all._
import fs2.Stream
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.{QueryableStoreTypes, Stores}
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite
import mtest.kafka._
import scala.concurrent.duration._
import scala.util.Random
import scala.collection.JavaConverters._

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
    val res =
      for {
        _ <- Stream.eval(topic.send(Random.nextInt(3), s"a${Random.nextInt(1000)}"))
        _ <- Stream.sleep(1.seconds)
        q <- ctx.buildStreams(top).runStoreQuery(sq)
        g <- ctx.buildStreams(gtop).runStoreQuery(gsq)
        _ <- Stream.sleep(1.seconds)
      } yield q.all().asScala.toList ++ g.all.asScala.toList
    println(ctx.buildStreams(top).topology.describe())
    println(res.compile.toList.unsafeRunSync().flatten)
  }
}
