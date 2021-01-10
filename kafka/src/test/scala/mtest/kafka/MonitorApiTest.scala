package mtest.kafka

import cats.effect.IO
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration._
import scala.util.Random

class MonitorApiTest extends AnyFunSuite {
  val topic = ctx.topic[Int, Int]("monitor.test")
  val tgt   = ctx.topic[Int, Int]("monitor.carbon.copy.test")

  val st = ctx.topic[Int, Array[Byte]]("monitor.test")

  val data = List(
    st.fs2PR(0, Array(0, 0, 0, 1)),
    st.fs2PR(1, Array(0, 0, 0, 2)),
    st.fs2PR(2, Array(0, 0)),
    st.fs2PR(3, Array(0, 0, 0, 4)),
    st.fs2PR(4, Array(0, 0, 0, 5)),
    st.fs2PR(5, Array(0, 0, 0, 6)),
    st.fs2PR(6, Array(0, 0, 0, 7))
  )

  val sender = Stream.awakeEvery[IO](1.seconds).zipRight(Stream.emits(data)).evalMap(st.send)

  test("realtime filter and watch") {
    val e   = Stream.eval(topic.withGroupId("g1").monitor.filterFromEarliest(_.key().toOption.exists(_ == 2)))
    val n   = Stream.eval(topic.withGroupId("g2").monitor.filter(_.key().toOption.exists(_ < 2)))
    val w   = Stream.eval(topic.withGroupId("g3").monitor.watch)
    val wn  = Stream.eval(topic.withGroupId("g4").monitor.watchFromEarliest)
    val wf  = Stream.eval(topic.withGroupId("g5").monitor.watchFrom(s"${Instant.now.toString}"))
    val bw  = Stream.eval(topic.withGroupId("g6").monitor.badRecords)
    val bwf = Stream.eval(topic.withGroupId("g7").monitor.badRecordsFromEarliest)

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
      .unsafeRunSync()
  }
  test("carbon copy") {
    topic.monitor.carbonCopyTo(tgt).unsafeRunTimed(3.seconds)
  }

  test("summary") {
    topic.monitor.summaries.unsafeRunSync()
    tgt.monitor.summaries.unsafeRunSync()
  }
}
