package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.{
  ActionStart,
  ActionSucc,
  PassThrough,
  ServiceStart,
  ServiceStop
}
import cron4s.Cron
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class AwakeEveryTest extends AnyFunSuite {
  val service = TaskGuard[IO]("awake").service("every")
  val cron    = Cron.unsafeParse("0-59 * * ? * *")
  test("1. should not lock - even") {
    val List(a, b, c, d) = service
      .eventStream(agent =>
        agent
          .awakeEvery(cron)
          .evalMap(idx => agent.action("even", _.notice).retry(IO(idx)).run)
          .compile
          .drain)
      .take(4)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionSucc])
    assert(d.isInstanceOf[ActionStart])
  }
  test("2. should not lock - odd") {
    val List(a, b, c, d, e) = service
      .eventStream(agent =>
        agent.awakeEvery(cron).evalMap(idx => agent.action("odd", _.notice).retry(IO(idx)).run).compile.drain)
      .take(5)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionSucc])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionSucc])
  }

  test("3. policy based awakeEvery") {
    val policy = policies.cronBackoff[IO](cron, ZoneId.systemDefault()).join(RetryPolicies.limitRetries(3))
    val List(a, b, c, d, e, f, g, h) =
      service
        .eventStream(agent =>
          awakeEvery(policy)
            .evalMap(idx => agent.action("policy", _.notice).retry(IO(idx)).run)
            .compile
            .drain)
        .compile
        .toList
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionSucc])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionSucc])
    assert(f.isInstanceOf[ActionStart])
    assert(g.isInstanceOf[ActionSucc])
    assert(h.isInstanceOf[ServiceStop])
  }

  test("4.cron index") {
    val lst = service
      .eventStream(ag =>
        ag.awakeEvery(Cron.unsafeParse("0-59 * * ? * *"))
          .evalMap(x => ag.broker("pt").passThrough(x.asJson))
          .take(3)
          .compile
          .drain)
      .compile
      .toList
      .map(_.filter(_.isInstanceOf[PassThrough]))
      .unsafeRunSync()
    assert(List(0, 1, 2) == lst.flatMap(_.asInstanceOf[PassThrough].value.asNumber.flatMap(_.toLong)))
  }

  test("5. fib awakeEvery") {
    val policy = RetryPolicies.fibonacciBackoff[IO](1.second).join(RetryPolicies.limitRetries(3))
    val List(a, b, c, d, e, f, g, h) =
      service
        .eventStream(agent =>
          awakeEvery(policy).evalMap(idx => agent.action("fib", _.notice).retry(IO(idx)).run).compile.drain)
        .compile
        .toList
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionSucc])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionSucc])
    assert(f.isInstanceOf[ActionStart])
    assert(g.isInstanceOf[ActionSucc])
    assert(h.isInstanceOf[ServiceStop])
  }
}
