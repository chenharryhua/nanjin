package mtest.guard

import cats.effect.IO
import com.github.chenharryhua.nanjin.guard.TaskGuard
import munit.CatsEffectSuite
import squants.information.Bytes

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class Performance extends CatsEffectSuite {
  // sbt "guard/testOnly mtest.guard.Performance"

  private val service = TaskGuard[IO]("performance").service("performance")

  private val timeout: FiniteDuration = 5.seconds

  test("1.performance - retry") {
    var i: Int = 0
    service
      .eventStream(_.retry(identity).use(_(IO(i += 1)).foreverM))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"retry:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("2.performance counter") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("counter")(_.counter("counter").use(_.inc(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"counter:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("3.performance meter") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("meter")(_.meter("meter").use(_.mark(1).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"meter:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("4.performance histogram") {
    var i: Int = 0
    service
      .eventStream(
        _.facilitate("histogram")(_.histogram("histogram", _.withUnit(Bytes)).use(_.update(1000).map(_ =>
          i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"histogram:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("5.performance timer") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("timer")(_.timer("timer").use(_.elapsedNano(1000).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"timer:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("6.performance timer - timing") {
    var i: Int = 0
    service
      .eventStream(_.facilitate("timer")(_.timer("timer").use(_.timing(IO(1000)).map(_ => i += 1).foreverM)))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"timing:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("7.performance circuit breaker") {
    var i: Int = 0
    service
      .eventStream(
        _.circuitBreaker(maxFailures = 5, _.empty).use { cb =>
          cb.protect(IO(i += 1)).foreverM
        }
      )
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"circuit breaker:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("8.performance batch light") {
    var i: Int = 0
    service
      .eventStream(agent => agent.batchLight("batch").monadic(_("j", IO(i += 1))).monadicBatch.foreverM)
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"batch light:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("8.performance batch traced") {
    var i: Int = 0
    service
      .eventStream(agent =>
        agent.batchTraced("batch traced", _.build).monadic(_("j", IO(i += 1))).monadicBatch.foreverM)
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .map { _ =>
        println(s"batch traced:  ${timeout.toNanos / i} nano")
        println(s"speed: ${i / timeout.toMillis} k/s")
      }
  }

  test("10.performance channel") {
    service
      .eventStreamS(agent => fs2.Stream.repeatEval(agent.logger.error("hello")).take(3_000_000))
      .compile
      .fold(0)((s, _) => s + 1)
      .timed
      .map { case (fd, i) =>
        println(s"channel:  ${fd.toNanos / i} nano")
        println(s"speed: ${i / fd.toMillis} k/s")
      }
  }
}
