package mtest.guard

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.implicits.{toFunctorFilterOps, toShow}
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, Policy, Tick}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

class ServiceTest extends AnyFunSuite {

  val guard: TaskGuard[IO] = TaskGuard[IO]("service-level-guard").updateConfig(
    _.withHomePage("https://abc.com/efg")
      .withZoneId(londonTime)
      .withRestartPolicy(Policy.fixedDelay(1.seconds))
      .addBrief(Json.fromString("test")))

  val policy: Policy = Policy.fixedDelay(0.1.seconds).limited(3)

  test("1.should stopped if the operation normally exits") {
    guard.service("exit").eventStream(_ => IO(())).compile.drain.unsafeRunSync()
  }

  test("2.escalate to up level if retry failed") {
    val List(a, b, c, d, e, f, g, h) = guard
      .service("retry")
      .updateConfig(_.withRestartPolicy(_.fixedDelay(1.seconds).limited(1)))
      .eventStream(ga =>
        ga.retry(_.withPolicy(_.fixedDelay(1.seconds).limited(1)))
          .use(_(ga.herald.info("info") *> IO.raiseError(new Exception))))
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServicePanic])
    assert(e.isInstanceOf[ServiceStart])
    assert(f.isInstanceOf[ServiceMessage])
    assert(g.isInstanceOf[ServiceMessage])
    assert(h.isInstanceOf[ServiceStop])
  }

  test("3.force reset") {
    val s :: b :: c :: _ = guard
      .service("reset")
      .updateConfig(_.withMetricReport(_.giveUp))
      .eventStream(ag => ag.adhoc.reset >> ag.adhoc.reset)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReset])
    assert(c.isInstanceOf[MetricReset])
  }

  test("4.policy start over") {

    val p1 = Policy.fixedDelay(1.seconds).limited(1)
    val p2 = Policy.fixedDelay(2.seconds).limited(1)
    val p3 = Policy.fixedDelay(3.seconds).limited(1)
    val policy = p1.followedBy(p2).followedBy(p3).repeat
    println(policy.show)
    val List(a, b, c, d, e, f, g, h) = guard
      .service("start over")
      .updateConfig(_.withRestartPolicy(policy))
      .eventStream(_ => IO.raiseError[Int](new Exception("oops")).void)
      .map(checkJson)
      .evalMapFilter[IO, Tick] {
        case sp: ServicePanic => IO(Some(sp.tick))
        case _                => IO(None)
      }
      .take(8)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.index == 1)
    assert(b.index == 2)
    assert(c.index == 3)
    assert(d.index == 4)
    assert(e.index == 5)
    assert(f.index == 6)
    assert(g.index == 7)
    assert(h.index == 8)

    assert(b.previous == a.wakeup)
    assert(c.previous == b.wakeup)
    assert(d.previous == c.wakeup)
    assert(e.previous == d.wakeup)
    assert(f.previous == e.wakeup)
    assert(g.previous == f.wakeup)
    assert(h.previous == g.wakeup)

    assert(a.snooze == 1.second.toJava)
    assert(b.snooze == 2.second.toJava)
    assert(c.snooze == 3.second.toJava)
    assert(d.snooze == 1.second.toJava)
    assert(e.snooze == 2.second.toJava)
    assert(f.snooze == 3.second.toJava)
    assert(g.snooze == 1.second.toJava)
    assert(h.snooze == 2.second.toJava)
  }

  test("5.policy threshold start over") {

    val policy: Policy = Policy.fixedDelay(1.seconds, 2.seconds, 3.seconds, 4.seconds, 5.seconds)
    println(policy)
    val List(a, b, c) =
      fs2.Stream
        .eval(AtomicCell[IO].of(0.seconds))
        .flatMap { box =>
          guard
            .service("threshold")
            .updateConfig(_.withRestartPolicy(policy))
            .updateConfig(_.withRestartThreshold(2.seconds))
            .eventStream { _ =>
              box.getAndUpdate(_ + 1.second).flatMap(IO.sleep) <*
                IO.raiseError[Int](new Exception("oops"))
            }
            .map(checkJson)
            .evalMapFilter[IO, Tick] {
              case sp: ServicePanic => IO(Some(sp.tick))
              case _                => IO(None)
            }
        }
        .take(3)
        .compile
        .toList
        .unsafeRunSync()

    assert(a.index == 1)
    assert(b.index == 2)
    assert(c.index == 3)

    assert(b.previous == a.wakeup)
    assert(c.previous == b.wakeup)

    assert(a.snooze == 1.second.toJava)
    assert(b.snooze == 2.second.toJava)
    assert(c.snooze == 1.second.toJava)
  }

  test("6.service config") {
    TaskGuard[IO]("abc")
      .service("abc")
      .updateConfig(
        _.withRestartPolicy(Policy.fixedDelay(1.second))
          .withMetricReset(Policy.giveUp)
          .withMetricReport(Policy.crontab(crontabs.secondly), 1)
          .withMetricDailyReset
          .withRestartThreshold(2.second))
      .eventStreamR(_.facilitate("nothing")(_.counter("counter")))
      .map(checkJson)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("7.throw exception in construction") {
    val List(a, b) = guard
      .service("simple")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { _ =>
        val c = true
        val err: Int = if (c) throw new Exception else 1
        IO.println(err)
      }
      .map(checkJson)
      .debug()
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ServiceStop].cause.asInstanceOf[ServiceStopCause.ByException].error.stack.nonEmpty)
  }

  test("8. closure - io") {
    val List(a, b) = guard
      .service("closure")
      .updateConfig(_.withRestartPolicy(_.fixedDelay(1.seconds).limited(1)))
      .eventStream { agent =>
        val a = UUID.randomUUID()
        agent.herald.info(a.toString) *> IO.raiseError(new Exception)
      }
      .mapFilter(eventFilters.serviceMessage)
      .debug()
      .compile
      .toList
      .unsafeRunSync()
    assert(a.message.as[String].toOption.get != b.message.as[String].toOption.get)
  }

  test("9. closure - stream") {
    val List(a, b) = guard
      .service("closure")
      .updateConfig(_.withRestartPolicy(_.fixedDelay(1.seconds).limited(1)))
      .eventStreamS { agent =>
        val a = UUID.randomUUID()
        fs2.Stream(0).covary[IO].evalMap(_ => agent.herald.info(a.toString) *> IO.raiseError(new Exception))
      }
      .mapFilter(eventFilters.serviceMessage)
      .debug()
      .compile
      .toList
      .unsafeRunSync()
    assert(a.message.as[String].toOption.get != b.message.as[String].toOption.get)

  }

  test("10.exception throw by java") {
    val res = guard
      .service("ex")
      .updateConfig(_.withRestartPolicy(_.fixedRate(1.seconds).limited(1)))
      .eventStream { _ =>
        assert(1 == 2)
        IO.unit
      }
      .compile
      .toList
      .unsafeRunSync()
    assert(res.size == 4)
    assert(res.head.isInstanceOf[ServiceStart])
    assert(res(1).isInstanceOf[ServicePanic])
    assert(res(2).isInstanceOf[ServiceStart])
    assert(res(3).asInstanceOf[ServiceStop].cause.isInstanceOf[ServiceStopCause.ByException])
  }

  test("11. exception thrown elsewhere") {
    val res = guard
      .service("ex")
      .updateConfig(_.withRestartPolicy(_.fixedRate(1.seconds).limited(1)))
      .eventStream { _ =>
        Future[Int] {
          Thread.sleep(2_000)
          println("throw exception")
          throw new Exception("oops")
        }(scala.concurrent.ExecutionContext.Implicits.global)
        IO.sleep(5.seconds)
      }
      .compile
      .toList
      .unsafeRunSync()
    assert(res.head.isInstanceOf[ServiceStart])
    assert(res(1).asInstanceOf[ServiceStop].cause == ServiceStopCause.Successfully)
  }
}
