package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration.*

class MonitorApiTest extends AnyFunSuite {
  val topic = ctx.topic[Int, Int]("monitor.test")
  val tgt   = ctx.topic[Int, Int]("monitor.carbon.copy.test")

  val st = ctx.topic[Int, Array[Byte]]("monitor.test")

  val sender = Stream
    .emits(
      List(
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 0, Array(0, 0, 0, 1)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 1, Array(0, 0, 0, 2)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 2, Array(0, 0)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 3, Array(0, 0, 0, 4)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 4, Array(0, 0, 0, 5)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 5, Array(0, 0, 0, 6)),
        ProducerRecord[Int, Array[Byte]](st.topicName.value, 6, Array(0, 0, 0, 7))
      ).map(ProducerRecords.one(_)))
    .covary[IO]
    .through(st.fs2Channel.producerPipe)

  test("realtime filter and watch") {
    val e   = topic.monitor.filterFromEarliest(_.key().toOption.exists(_ == 2))
    val n   = topic.monitor.filter(_.key().toOption.exists(_ < 2))
    val w   = topic.monitor.watch
    val wn  = topic.monitor.watchFromEarliest
    val wf  = topic.monitor.watchFrom(s"${Instant.now.toString}")
    val bw  = topic.monitor.badRecords
    val bwf = topic.monitor.badRecordsFromEarliest

    IO.parSequenceN(7)(List(e, n, w, wn, wf, bw, bwf)).unsafeRunTimed(10.seconds)
  }
  test("carbon copy") {
    topic.monitor.carbonCopyTo(tgt).unsafeRunTimed(3.seconds)
  }

  test("summary") {
    topic.monitor.summaries.unsafeRunSync()
    tgt.monitor.summaries.unsafeRunSync()
  }
}
