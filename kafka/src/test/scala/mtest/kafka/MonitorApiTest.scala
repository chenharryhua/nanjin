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

  val sender = Stream.awakeEvery[IO](1.seconds).zipWithIndex.evalMap { case (_, idx) =>
    topic.send(idx.toInt, Random.nextInt(9))
  }

  test("realtime filter and watch") {
    val e   = Stream.eval(topic.monitor.filterFromEarliest(_.key().toOption.exists(_ == 2)))
    val n   = Stream.eval(topic.monitor.filter(_.key().toOption.exists(_ < 2)))
    val w   = Stream.eval(topic.monitor.watch)
    val wn  = Stream.eval(topic.monitor.watchFromEarliest)
    val wf  = Stream.eval(topic.monitor.watchFrom(s"${Instant.now.toString}"))
    val bw  = Stream.eval(topic.monitor.badRecords)
    val bwf = Stream.eval(topic.monitor.badRecordsFromEarliest)

    sender
      .concurrently(e)
      .concurrently(n)
      .concurrently(w)
      .concurrently(wn)
      .concurrently(wf)
      .concurrently(bw)
      .concurrently(bwf)
      .interruptAfter(5.seconds)
      .compile
      .drain
      .unsafeRunSync()
  }
  test("carbon copy") {
    Stream.eval(topic.monitor.carbonCopyTo(tgt)).interruptAfter(3.seconds).compile.drain.unsafeRunSync()
  }

  test("summary") {
    topic.monitor.summaries.unsafeRunSync()
    tgt.monitor.summaries.unsafeRunSync()
  }

}
