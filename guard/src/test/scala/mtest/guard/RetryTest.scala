package mtest.guard

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.ServiceStopCause.Successfully
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.Agent
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class RetryTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("retry").service("retry")

  test("1.retry - simplest") {
    service.eventStream(_.retry(identity).use(_(IO(())))).compile.drain.unsafeRunSync()
  }

  test("2.retry - give up") {
    val mr = service.eventStream { agent =>
      agent.retry(_.withPolicy(_.giveUp)).use(_(IO(()) *> agent.adhoc.report))
    }.map(checkJson).mapFilter(eventFilters.metricReport).compile.toList.unsafeRunSync()
    assert(mr.head.snapshot.isEmpty)
  }

  test("3.retry - fail") {
    val action = IO.raiseError[Int](new Exception())
    var i = 0
    service.eventStream { agent =>
      agent
        .retry(_.withPolicy(_.fixedDelay(1.second).limited(2)).isWorthRetry { _ =>
          i += 1; IO(true)
        })
        .use(_(action) *> agent.adhoc.report)
    }.map(checkJson).compile.toList.unsafeRunSync()
    assert(i == 3)
  }

  test("4.retry - success after retry") {
    var i = 0
    val action = IO(i += 1) >> { if (i < 2) throw new Exception(i.toString) else IO(0) }

    val policy = Policy.fixedDelay(1.second, 100.seconds).limited(20)

    val ss = service.eventStream { agent =>
      agent.retry(_.withPolicy(policy)).use(_(action) <* agent.adhoc.report).map(x => assert(x == 0)).void
    }.map(checkJson).mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(ss.cause == Successfully)
  }

  test("5.retry - unworthy") {
    val action = IO.raiseError[Int](new Exception())
    service
      .eventStream(
        _.retry(_.withPolicy(_.fixedDelay(100.seconds)).isWorthRetry(_ => IO(false))).use(_(action)).void)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("6.retry - simple cancellation") {
    service
      .eventStream(agent =>
        agent.retry(_.withPolicy(Policy.giveUp)).use { retry =>
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

  test("7.retry - cancellation internal") {
    def action(agent: Agent[IO]) = for {
      counter <- agent.facilitate("retry")(_.counter("total.calls"))
      retry <- agent.retry(_.withPolicy(_.giveUp))
    } yield (in: IO[Unit]) =>
      IO.uncancelable(poll =>
        in *>
          IO.println("before retry") *>
          counter.inc(1) *>
          retry(poll(in)) *>
          IO.println("after retry"))

    service
      .eventStream(agent =>
        agent.facilitate("retry.internal.cancellation")(_ => action(agent)).use { retry =>
          (retry(IO.println("first")) >>
            IO.println("----") >>
            retry(IO.println("before cancel") >> IO.canceled >> IO.println("after cancel")) >>
            retry(IO.println("third"))).guarantee(agent.adhoc.report)
        })
      .mapFilter(eventFilters.metricReport)
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("8.retry - cancellation external") {
    def action(agent: Agent[IO]) = for {
      counter <- agent.facilitate("retry")(_.counter("total.calls"))
      retry <- agent.retry(_.withPolicy(_.fixedDelay(10.hours)))
    } yield (in: IO[Unit]) =>
      IO.uncancelable(poll =>
        IO.println("before retry") *>
          counter.inc(1) *>
          poll(retry(in)) *> // retry(poll(in)) will wait 10 hours
          IO.println("after retry"))

    service
      .eventStream(agent =>
        agent.facilitate("retry.external.cancellation")(_ => action(agent)).use { retry =>
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

  test("9.retry resource") {
    var i = 0
    val resource =
      Resource
        .make(IO(1))(_ => IO(i += 1) >> IO.println("released"))
        .evalTap(_ => IO.raiseError(new Exception()))
    service.eventStreamR { agent =>
      agent.retry(_.withPolicy(_.fixedDelay(1.second).limited(2))).flatMap { retry =>
        retry(resource)
      }
    }.compile.drain.unsafeRunSync()
    assert(i == 3)
  }
}
