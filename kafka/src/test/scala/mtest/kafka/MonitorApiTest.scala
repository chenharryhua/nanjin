package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class MonitorApiTest extends AnyFunSuite {
  private val topicDef: AvroTopic[Int, Int] = AvroTopic[Int, Int](TopicName("monitor.test"))

  private val st: AvroTopic[Int, Array[Byte]] =
    AvroTopic[Int, Array[Byte]](TopicName("monitor.test"))

  private val headers1: Headers = Headers.fromSeq(List(Header("a", "aaaaa")))
  val headers2: Headers =
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
    .through(ctx.produce[Int, Array[Byte]](st.pair).sink)

  test("monitor") {
    ctx.schemaRegistry.register(topicDef).attempt.unsafeRunSync()
    sender
      .concurrently(ctx.monitor("monitor.test").debug())
      .interruptAfter(8.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
}
