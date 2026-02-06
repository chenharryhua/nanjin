package mtest.guard

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.ServiceStopCause.{ByCancellation, Successfully}
import com.github.chenharryhua.nanjin.guard.event.{eventFilters, retrieveCounter}
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

  test("3.retry - always fail") {
    var j = 0 // total calls of action
    val action = IO(j += 1) >> IO.raiseError[Unit](new Exception())
    var i = 0 // retry count
    service.eventStream { agent =>
      val retry = agent.retry(_.withPolicy(_.fixedDelay(1.second).limited(3)).withDecision { tv =>
        i += 1
        IO.println(tv).as(tv.map(_ => true))
      })

      retry.use(_(action))
    }.map(checkJson).compile.toList.unsafeRunSync()
    assert(i == 3)
    assert(j == 4)
  }

  test("4.retry - success after retry") {
    var i = 0
    val action = IO(i += 1) >> { if (i < 2) throw new Exception(i.toString) else IO(0) }

    val ss = service.eventStream { agent =>
      val retry = agent.retry(_.withPolicy(_.fixedDelay(1.second, 100.seconds).limited(20)))

      retry.use(_(action)).map(x => assert(x == 0)).void
    }.map(checkJson).mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(ss.cause == Successfully)
    assert(i == 2)
  }

  test("5.retry - unworthy") {
    var i = 0
    val action = IO(i += 1) >> IO.raiseError[Int](new Exception("unworthy retry"))
    val res = service.eventStream { agent =>
      val retry =
        agent.retry(_.withPolicy(_.fixedDelay(100.seconds)).withDecision(tv => IO(tv.map(_ => false))))
      retry.use(_(action)).void
    }.mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(res.cause.exitCode == 3)
    assert(i == 1)
  }

  test("6.retry - simple cancellation") {
    val res = service
      .eventStream(agent =>
        agent.retry(_.withPolicy(Policy.giveUp)).use { retry =>
          (retry(IO.println(1)) >>
            retry(IO.println(2) <* IO.canceled *> IO.println(3)) >>
            retry(IO.println(4))).guarantee(agent.adhoc.report)
        })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(res.cause == ByCancellation)
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

    val ss = service
      .eventStream(agent =>
        agent.facilitate("retry.internal.cancellation")(_ => action(agent)).use { retry =>
          (retry(IO.println("first")) >>
            IO.println("----") >>
            retry(IO.println("before cancel") >> IO.canceled >> IO.println("after cancel")) >>
            retry(IO.println("third"))).guarantee(agent.adhoc.report)
        })
      .mapFilter(eventFilters.serviceStop)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(ss.cause == ByCancellation)
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

    val ss = service
      .eventStream(agent =>
        agent.facilitate("retry.external.cancellation")(_ => action(agent)).use { retry =>
          IO.race(retry(IO.println("before exception") >> IO.raiseError(new Exception)), IO.sleep(3.seconds))
            .void
            .guarantee(agent.adhoc.report)
        })
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()

    assert(retrieveCounter(ss.snapshot.counters).head._2 == 1)
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

  test("10. conditional retry") {
    var i = 0
    val action = IO(i += 1) <* IO.raiseError(new Exception)
    val ss = service.eventStream { agent =>
      val retry = agent.retry(_.withPolicy(_.fixedDelay(1.second)).withDecision { tv =>
        IO(tv.map(_ => tv.tick.index < 2))
      })
      retry.use(_(action))
    }.mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()

    assert(i == 2)
    assert(ss.cause.exitCode == 3)
  }

  test("11. conditional retry") {
    var i = 0
    val action = IO(i += 1) <* IO.raiseError(new Exception)
    val ss = service.eventStream { agent =>
      val retry = agent.retry(_.withPolicy(_.fixedDelay(1.second)).withDecision { tv =>
        val decision = (tv.value, tv.tick.index) match {
          case (_: Exception, 1) => true
          case (_: Exception, 2) => true
          case (_: Exception, 3) => true
          case (_, _)            => false
        }
        IO(tv.map(_ => decision))
      })
      retry.use(_(action))
    }.mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()

    assert(i == 4)
    assert(ss.cause.exitCode == 3)
  }
}
