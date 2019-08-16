package mtest

import org.scalatest.FunSuite

class ListingTest extends FunSuite {
  test("listing to") {
    second_topic.watchFromLatest.unsafeRunSync()
  }
}
