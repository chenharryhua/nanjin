package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{eventFilters, retrieveTimer}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class RetryTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("retry").service("retry")

  test("1.retry - simplest") {
    service
      .eventStream(_.facilitate("retry")(_.measuredRetry(identity)).use(_(IO(()))))
      .compile
      .drain
      .unsafeRunSync()
  }

  test("2.retry - enable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("retry")(_.measuredRetry(_.withPolicy(_.giveUp).enable(true)))
        .use(_(IO(()) *> agent.adhoc.report))
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.toList.unsafeRunSync()
    assert(mr.head.snapshot.nonEmpty)
  }

  test("3.retry - disable") {
    val mr = service.eventStream { agent =>
      agent
        .facilitate("retry")(_.measuredRetry(_.withPolicy(_.giveUp).enable(false)))
        .use(_(IO(()) *> agent.adhoc.report))
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.toList.unsafeRunSync()
    assert(mr.head.snapshot.isEmpty)
  }

  test("4.retry - fail") {
    val action = IO.raiseError[Int](new Exception())
    var i      = 0
    service.eventStream { agent =>
      agent
        .facilitate("retry")(_.measuredRetry(_.withPolicy(_.fixedDelay(1.second).limited(2)).worthRetry { _ =>
          i += 1; IO(true)
        }))
        .use(_(action) *> agent.adhoc.report)
    }.map(checkJson).compile.toList.unsafeRunSync()
    assert(i == 3)
  }

  test("5.retry - success after retry") {
    var i = 0
    val action = IO(i += 1) >> IO.defer {
      if (i < 2)
        IO.raiseError[Int](new Exception())
      else IO(0)
    }

    val policy = Policy.fixedDelay(1.second, 100.seconds).limited(20)

    val mr = service.eventStream { agent =>
      agent
        .facilitate("retry")(_.measuredRetry(_.withPolicy(policy)))
        .use(_(action) <* agent.adhoc.report)
        .map(x => assert(x == 0))
        .void
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.toList.unsafeRunSync()

    assert(retrieveTimer(mr.head.snapshot.timers).head._2.calls == 1)
  }

  test("6.retry - unworthy") {
    val action = IO.raiseError[Int](new Exception())
    service
      .eventStream(_.facilitate("retry")(_.measuredRetry(
        _.withPolicy(_.fixedDelay(100.seconds)).enable(true).worthRetry(_ => IO(false)))).use(_(action)).void)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("7.performance - measured") {
    var i: Int  = 0
    val timeout = 5.seconds
    service
      .eventStream(_.facilitate("performance")(_.measuredRetry(_.enable(true))).use(_(IO(i += 1)).foreverM))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("8.performance - pure") {
    var i: Int  = 0
    val timeout = 5.seconds
    service
      .eventStream(_.createRetry(Policy.giveUp).use(_(IO(i += 1)).foreverM))
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }

  test("9.performance - wrong") {
    var i: Int  = 0
    val timeout = 5.seconds
    service
      .eventStream(_.facilitate("performance")(_.measuredRetry(_.enable(true))).use(_(IO(i += 1))).foreverM)
      .timeoutOnPullTo(timeout, fs2.Stream.empty)
      .compile
      .drain
      .unsafeRunSync()

    println(s"cost: ${timeout.toNanos / i} nano")
    println(s"speed: ${i / timeout.toMillis} calls/milli")
  }
}
