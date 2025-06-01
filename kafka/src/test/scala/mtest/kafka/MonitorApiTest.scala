package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.{KafkaTopic, TopicDef}
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.*
import org.apache.kafka.common.TopicPartition
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class MonitorApiTest extends AnyFunSuite {
  private val topicDef: TopicDef[Int, Int]    = TopicDef[Int, Int](TopicName("monitor.test"))
  private val topic: KafkaTopic[IO, Int, Int] = ctx.topic[Int, Int](topicDef)

  private val st: KafkaTopic[IO, Int, Array[Byte]] =
    ctx.topic(TopicDef[Int, Array[Byte]](TopicName("monitor.test")))

  private val headers1: Headers = Headers.fromSeq(List(Header("a", "aaaaa")))
  val headers2: Headers         =
    Headers.fromSeq(List(Header("b", ""), Header("warn", "value is null as expected")))

  private val sender: Stream[IO, ProducerResult[Int, Array[Byte]]] = Stream
    .emits(
      List(
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 0, Array(0, 0, 0, 1)).withHeaders(headers1),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 1, Array(0, 0, 0, 2)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 3, Array(0, 0, 0, 4)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 4, Array(0, 0, 0, 5)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 5, Array(0, 0, 0, 6)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 6, Array(0, 0, 0, 7))
      ).map(ProducerRecords.one))
    .covary[IO]
    .chunkN(1)
    .unchunks
    .metered(1.seconds)
    .through(ctx.producer[Int, Array[Byte]].sink)

  test("monitor") {
    ctx.schemaRegistry.register(topic.topicDef).attempt.unsafeRunSync()
    sender
      .concurrently(ctx.monitor("monitor.test").debug())
      .interruptAfter(8.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("cherry pick") {
    ctx
      .admin("monitor.test")
      .use(_.offsetRangeFor(DateTimeRange(sydneyTime)))
      .flatMap { kor =>
        val range = kor.get(new TopicPartition("monitor.test", 0)).flatten.get
        ctx.cherryPick("monitor.test", 0, range.from.value)
      }
      .unsafeRunSync()
  }

}
