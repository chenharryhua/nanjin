package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{ActionDone, ActionStart, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.*

class TicksTest extends AnyFunSuite {
  val service: ServiceGuard[IO] = TaskGuard[IO]("awake").service("every")
  test("1. should not lock - even") {
    val List(a, b, c, d) = service
      .eventStream(agent =>
        agent
          .ticks(cron_1second)
          .evalMap(idx => agent.action("even", _.notice).retry(IO(idx)).run)
          .compile
          .drain)
      .take(4)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionDone])
    assert(d.isInstanceOf[ActionStart])
  }

  test("2. should not lock - odd") {
    val List(a, b, c, d, e) = service
      .eventStream(agent =>
        agent
          .ticks(cron_1second)
          .evalMap(idx => agent.action("odd", _.notice).retry(IO(idx)).run)
          .compile
          .drain)
      .take(5)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionDone])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionDone])
  }

  test("3. policy based awakeEvery") {
    val List(a, b, c, d, e, f, g, h) =
      service
        .eventStream(agent =>
          agent
            .ticks(cron_1second, _.join(RetryPolicies.limitRetries(3)))
            .evalMap(idx => agent.action("policy", _.notice).retry(IO(idx)).run)
            .compile
            .drain)
        .compile
        .toList
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionDone])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionDone])
    assert(f.isInstanceOf[ActionStart])
    assert(g.isInstanceOf[ActionDone])
    assert(h.isInstanceOf[ServiceStop])
  }

  test("4.cron index") {

    val lst = service
      .eventStream(ag =>
        ag.ticks(cron_1minute, RetryPolicies.capDelay[IO](1.second, _))
          .evalMap(x => ag.action("pt", _.aware).retry(IO(x.index.asJson)).logOutput(identity).run)
          .take(3)
          .compile
          .drain)
      .compile
      .toList
      .map(_.filter(_.isInstanceOf[ActionDone]))
      .unsafeRunSync()
    assert(List(1, 2, 3) == lst.flatMap(_.asInstanceOf[ActionDone].notes.get.asNumber.flatMap(_.toLong)))
  }

  test("5. fib awakeEvery") {
    val policy = RetryPolicies.fibonacciBackoff[IO](1.second).join(RetryPolicies.limitRetries(3))
    val List(a, b, c, d, e, f, g, h) =
      service
        .eventStream(agent =>
          agent.ticks(policy).evalMap(idx => agent.action("fib", _.notice).retry(IO(idx)).run).compile.drain)
        .compile
        .toList
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionDone])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionDone])
    assert(f.isInstanceOf[ActionStart])
    assert(g.isInstanceOf[ActionDone])
    assert(h.isInstanceOf[ServiceStop])
  }
}
