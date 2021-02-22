package mtest.kafka

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import fs2.Stream
import fs2.kafka.{ProducerRecord, ProducerRecords}
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.duration._
import scala.util.Random

class MonitorApiTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
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

  "Monitor" - {
    "realtime filter and watch" in {
      val e   = Stream.eval(topic.monitor.filterFromEarliest(_.key().toOption.exists(_ == 2)))
      val n   = Stream.eval(topic.monitor.filter(_.key().toOption.exists(_ < 2)))
      val w   = Stream.eval(topic.monitor.watch)
      val wn  = Stream.eval(topic.monitor.watchFromEarliest)
      val wf  = Stream.eval(topic.monitor.watchFrom(s"${Instant.now.toString}"))
      val bw  = Stream.eval(topic.monitor.badRecords)
      val bwf = Stream.eval(topic.monitor.badRecordsFromEarliest)

      sender
        .concurrently(bw)
        .concurrently(bwf)
        .concurrently(e)
        .concurrently(n)
        .concurrently(w)
        .concurrently(wn)
        .concurrently(wf)
        .interruptAfter(9.seconds)
        .compile
        .drain
        .assertNoException
    }

//    "carbon copy" in {
//      topic.monitor.carbonCopyTo(tgt).unsafeRunTimed(3.seconds)
//    }

    "summary" in {
      topic.monitor.summaries.assertNoException
      tgt.monitor.summaries.assertNoException
    }
  }
}
