package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Bytes

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class Performance extends AnyFunSuite {
  // sbt "guard/testOnly mtest.guard.Performance"

  private val service =
    TaskGuard[IO]("performance").service("performance").updateConfig(_.withLogFormat(_.Slf4j_Json_OneLine))

  private val timeout: FiniteDuration = 5.seconds

  test("1.performance - retry") {
    var i: Int = 0
    service
      .eventStream(_.retry(identity).use(_(IO(i += 1)).foreverM))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("2.performance counter") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("counter")(_.counter("counter").use(_.inc(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("3.performance permanent counter") {
    var i: Int = 0
    service
      .eventStream(
        _.facilitate("counter")(_.permanentCounter("permanent").use(_.inc(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("4.performance meter") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("meter")(_.meter("meter").use(_.mark(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("5.performance histogram") {
    var i: Int = 0
    service
      .eventStream(
        _.facilitate("histogram")(_.histogram("histogram", _.withUnit(Bytes)).use(_.update(1000).map(_ =>
          i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("6.performance timer") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("timer")(_.timer("timer").use(_.elapsedNano(1000).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("7.performance timer - timing") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("timer")(_.timer("timer").use(_.timing(IO(1000)).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("8.performance circuit breaker") {
    var i: Int = 0
    service
      .eventStream(
        _.circuitBreaker(identity).use { cb =>
          cb.protect(IO(i += 1)).foreverM
        }
      )
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }

  test("9.performance herald") {
    val (fd, i) = service
      .eventStreamS(agent => fs2.Stream.repeatEval(agent.herald.info("hello")).take(3_000_000))
      .compile
      .fold(0)((s, _) => s + 1)
      .timed
      .unsafeRunSync()

    println(s"cost:  ${fd.toNanos / i} nano")
    println(s"speed: ${i / fd.toMillis} k/s")
  }
}
