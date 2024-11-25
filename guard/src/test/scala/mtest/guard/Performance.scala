package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class Performance extends AnyFunSuite {
  private val service = TaskGuard[IO]("performance").service("performance")

  private val timeout: FiniteDuration = 5.seconds

  test("1.performance - measured enabled") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("retry.true")(_.measuredRetry(_.enable(true))).use(_(IO(i += 1)).foreverM))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("2.performance - measured disabled") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("retry.false")(_.measuredRetry(_.enable(false))).use(_(IO(i += 1)).foreverM))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("3.performance - wrong") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("wrong")(_.measuredRetry(_.enable(true))).use(_(IO(i += 1))).foreverM)
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("4.performance counter") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("counter")(_.counter("counter").use(_.inc(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("5.performance permanent counter") {
    var i: Int = 0
    service
      .eventStream(
        _.facilitate("counter")(_.permanentCounter("permanent").use(_.inc(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("6.performance meter") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("meter")(_.meter("meter").use(_.mark(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("7.performance histogram") {
    var i: Int = 0
    service
      .eventStream(
        _.facilitate("histogram")(_.histogram("histogram").use(_.update(1000).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("8.performance timer") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("timer")(_.timer("timer").use(_.update(1000).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }
}
