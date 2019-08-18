package mtest

import java.time.LocalDateTime
import io.circe.generic.auto._

import org.scalatest.FunSuite
case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)
case class StreamKey(name: Int)

class ListingTest extends FunSuite {
  test("listing to") {
    // ctx.topic[StreamKey, StreamTarget]("stream-target").watchFromLatest.unsafeRunSync()
    val end   = LocalDateTime.now
    val start = end.minusDays(2)
    //first_topic.monitor.saveJson(start, end, "my.json").unsafeRunSync()
    first_topic.monitor.watchFromEarliest.unsafeRunSync()
  }
}
