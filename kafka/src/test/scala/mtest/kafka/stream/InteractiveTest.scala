package mtest.kafka.stream

import cats.data.Reader
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings, TopicDef}
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.TimeoutException
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Random
@DoNotDiscover
class InteractiveTest extends AnyFunSuite {
  val appid = "interactive_test"
  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        .withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "nj-kafka-interactive-unit-test-group")
        .withStreamingProperty("state.dir", "./data/kafka_states"))

  val topic       = ctx.topic(TopicDef[Int, String](TopicName("stream.test.interactive.5")))
  val localStore  = topic.asStateStore("stream.test.interactive.local.store.5")
  val globalStore = topic.asStateStore("stream.test.interactive.store.global.5")

  val top: Reader[StreamsBuilder, Unit] =
    topic.asConsumer.withProcessorName("abc").ktable(localStore.inMemoryKeyValueStore.supplier).void
  val gtop: Reader[StreamsBuilder, Unit] =
    topic.asConsumer.gktable(globalStore.persistentKeyValueStore.supplier).void

  test("interactive") {
    val pr: ProducerRecords[Int, String] = ProducerRecords.one(
      ProducerRecord(topic.topicName.value, Random.nextInt(3), s"a${Random.nextInt(1000)}"))
    val feedData: Stream[IO, ProducerResult[Int, String]] =
      ctx.producer[Int, String].stream.evalMap(_.produce(pr).flatten)

    val res: Stream[IO, List[KeyValue[Int, String]]] =
      for {
        _ <- feedData
        kss1 <- ctx.buildStreams(appid, top.run).kafkaStreams
        kss2 <- ctx.buildStreams(appid, gtop).kafkaStreams
        _ <- Stream.sleep[IO](2.seconds)
      } yield {
        val g: List[KeyValue[Int, String]] =
          kss1.store(localStore.query.keyValueStore).all().asScala.toList.sortBy(_.key)
        val q: List[KeyValue[Int, String]] =
          kss2.store(globalStore.query.keyValueStore).all().asScala.toList.sortBy(_.key)
        assert(q === g)
        q
      }

    println(Console.CYAN + "interactive" + Console.RESET)
    println(ctx.buildStreams(appid, top).topology.describe())
    println(res.compile.toList.unsafeRunSync().flatten)
  }

  test("startup timeout") {
    println(Console.CYAN + "startup timeout" + Console.RESET)
    val to1 = ctx
      .buildStreams(appid, top)
      .withProperty(ConsumerConfig.GROUP_ID_CONFIG, "gid")
      .withStartUpTimeout(0.seconds)
      .stateUpdates
      .debug()
      .compile
      .drain
    assertThrows[TimeoutException](to1.unsafeRunSync())
    val to2 =
      ctx.buildStreams(appid, top).withStartUpTimeout(1.day).kafkaStreams.map(_.state()).debug().compile.drain
    to2.unsafeRunSync()
  }

  test("detect stream stop") {
    println(Console.CYAN + "detect stream stop" + Console.RESET)
    val to1 =
      ctx
        .buildStreams(appid, top)
        .kafkaStreams
        .evalMap(ks => IO.sleep(1.seconds) >> IO(ks.close()) >> IO.sleep(1.day))
    to1.compile.drain.unsafeRunSync()
  }

  test("detect stream error") {
    println(Console.CYAN + "detect stream error" + Console.RESET)
    val to1 =
      ctx
        .buildStreams(appid, top)
        .kafkaStreams
        .evalMap(ks => IO.sleep(1.seconds) >> IO(ks.cleanUp()) >> IO.sleep(1.day))
    assertThrows[IllegalStateException](to1.compile.drain.unsafeRunSync())
  }
}
