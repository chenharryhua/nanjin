package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Bytes

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class Performance extends AnyFunSuite {
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
      .eventStream(_.facilitate("meter")(_.meter(Bytes)("meter").use(_.mark(1).map(_ => i += 1).foreverM)))
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
      .eventStream(_.facilitate("histogram")(
        _.histogram(Bytes)("histogram").use(_.update(1000).map(_ => i += 1).foreverM)))
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
      .eventStream(_.facilitate("timer")(_.timer("timer").use(_.elapsed(1000).map(_ => i += 1).foreverM)))
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

  test("9.performance cache") {
    service
      .eventStream(_.caffeineCache(Caffeine.newBuilder().build[Int, Int]()).use { cache =>
        cache.put(1, 0) >>
          cache.updateWith(1)(_.fold(0)(_ + 1)).foreverM.timeout(timeout).attempt >>
          cache.get(1, IO(0)).flatMap { i =>
            IO.println(s"cost:  ${timeout.toNanos / i} nano") >>
              IO.println(s"speed: ${i / timeout.toMillis} k/s")
          }
      })
      .compile
      .drain
      .unsafeRunSync()
  }

  test("10.performance herald") {
    val i = service
      .eventStream(_.herald.info("hi").foreverM.timeout(timeout).attempt.void)
      .compile
      .fold(0L)((sum, _) => sum + 1)
      .unsafeRunSync()
    println(s"cost:  ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} k/s")
  }
}
