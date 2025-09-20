package mtest.kafka.stream

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.kafka.connector.commitBatch
import com.github.chenharryhua.nanjin.kafka.streaming.KafkaStreamsBuilder
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult}
import mtest.kafka.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
@DoNotDiscover
class TransformerTest extends AnyFunSuite {
  val appid = "transform_test"

  test("stream transformer") {
    // val store = ctx.store[Int, String]("stream.builder.test.store")

    def td: AvroTopic[Int, String] = AvroTopic[Int, String](TopicName("stream"))

    val topic1 = td.withTopicName("stream.builder.test.stream1")
    val topic2 = td.withTopicName("stream.builder.test.table2")
    val tgt = td.withTopicName("stream.builder.test.target")

    val kafkaStreamService: KafkaStreamsBuilder[IO] =
      ctx.buildStreams(appid)(apps.transformer_app)

    println(kafkaStreamService.topology.describe())

    val t2Data = Stream(
      ProducerRecords(
        List(
          ProducerRecord(topic2.topicName.value, 2, "t0"),
          ProducerRecord(topic2.topicName.value, 4, "t1"),
          ProducerRecord(topic2.topicName.value, 6, "t2")))).covary[IO].through(ctx.sharedProduce(td.pair).sink)

    val s1Data: Stream[IO, ProducerResult[Int, String]] =
      Stream
        .awakeEvery[IO](1.seconds)
        .zipWithIndex
        .map { case (_, index) =>
          ProducerRecords.one(ProducerRecord(topic1.topicName.value, index.toInt, s"stream$index"))
        }
        .through(ctx.sharedProduce[Int, String](td.pair).sink)

    val byteTopic = AvroTopic[Array[Byte], Array[Byte]](tgt.topicName)
    val havest = ctx
      .consume(byteTopic)
      .assign
      .map(ctx.serde(tgt).deserialize(_))
      .debug()
      .observe(_.map(_.offset).through(commitBatch(10, 2.seconds)).drain)

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
