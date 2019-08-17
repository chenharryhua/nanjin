package mtest

import org.scalatest.FunSuite
case class StreamTarget(oneName: String, twoName: String, size: Int, color: Int)
case class StreamKey(name: Int)

class ListingTest extends FunSuite {
  test("listing to") {
    // ctx.topic[StreamKey, StreamTarget]("stream-target").watchFromLatest.unsafeRunSync()
    first_topic.watchFromLatest.unsafeRunSync()
  }
}
