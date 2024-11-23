package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{eventFilters, retrieveTimer}
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import com.github.chenharryhua.nanjin.guard.observers.console
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

  test("7.retry - simple cancellation") {
    service
      .eventStream(agent =>
        agent.facilitate("cancel")(_.measuredRetry(_.withPolicy(Policy.giveUp))).use { retry =>
          (retry(IO.println(1)) >>
            retry(IO.println(2) <* IO.canceled *> IO.println(3)) >>
            retry(IO.println(4))).guarantee(agent.adhoc.report)
        })
      .mapFilter(eventFilters.metricReport)
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("8.retry - cancellation internal") {
    def action(mtx: Metrics[IO]) = for {
      counter <- mtx.counter("total.calls")
      retry <- mtx.measuredRetry(_.withPolicy(_.giveUp))
    } yield (in: IO[Unit]) =>
      IO.uncancelable(poll =>
        in *>
          IO.println("before retry") *>
          counter.inc(1) *>
          retry(poll(in)) *>
          IO.println("after retry"))

    service
      .eventStream(agent =>
        agent
          .facilitate("retry.internal.cancellation")(action)
          .use(retry =>
            (retry(IO.println("first")) >>
              IO.println("----") >>
              retry(IO.println("before cancel") >> IO.canceled >> IO.println("after cancel")) >>
              retry(IO.println("third"))).guarantee(agent.adhoc.report)))
      .mapFilter(eventFilters.metricReport)
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("9.retry - cancellation external") {
    def action(mtx: Metrics[IO]) = for {
      counter <- mtx.counter("total.calls")
      retry <- mtx.measuredRetry(_.withPolicy(_.fixedDelay(10.hours)))
    } yield (in: IO[Unit]) =>
      IO.uncancelable(poll =>
        IO.println("before retry") *>
          counter.inc(1) *>
          poll(retry(in)) *> // retry(poll(in)) will wait 10 hours
          IO.println("after retry"))

    service
      .eventStream(agent =>
        agent.facilitate("retry.external.cancellation")(action).use { retry =>
          IO.race(retry(IO.println("before exception") >> IO.raiseError(new Exception)), IO.sleep(3.seconds))
            .void
            .guarantee(agent.adhoc.report)
        })
      .mapFilter(eventFilters.metricReport)
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("10.performance - measured") {
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

  test("11.performance - pure") {
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

  test("12.performance - wrong") {
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
