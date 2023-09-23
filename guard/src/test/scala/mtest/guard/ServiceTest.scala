package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy, Tick}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import cron4s.Cron
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.ControlThrowable

class ServiceTest extends AnyFunSuite {

  val guard: ServiceGuard[IO] = TaskGuard[IO]("service-level-guard")
    .updateConfig(_.withHomePage("https://abc.com/efg").withZoneId(londonTime))
    .service("service")
    .withRestartPolicy(policies.constant(1.seconds))
    .withBrief(Json.fromString("test"))

  val policy: Policy = policies.constant(0.1.seconds).limited(3)

  test("1.should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .withRestartPolicy(policies.constant(3.seconds))
      .withMetricReport(policies.crontab(cron_1hour))
      .withMetricDailyReset
      .eventStream(gd => gd.action("t", _.silent).delay(1).logOutput(_ => null).run.delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(d.isInstanceOf[ServiceStop])
    assert(d.asInstanceOf[ServiceStop].cause.exitCode == 0)
  }

  test("2.escalate to up level if retry failed") {
    val Vector(s, a, b, c, d, e, f) = guard
      .withRestartPolicy(policies.jitter(30.minutes, 50.minutes))
      .eventStream { gd =>
        gd.action("t", _.notice).withRetryPolicy(policy).retry(IO.raiseError(new Exception("oops"))).run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("3.should stop when fatal error occurs") {
    val List(a, b, c, d) = guard
      .withRestartPolicy(policies.crontab(Cron.unsafeParse("0-59 * * ? * *")))
      .eventStream { gd =>
        gd.action("fatal error", _.notice)
          .withRetryPolicy(policy)
          .retry(IO.raiseError(new ControlThrowable("fatal error") {}))
          .run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .evalTap(console.simple[IO])
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ServiceStop])
    assert(d.asInstanceOf[ServiceStop].cause.exitCode == 2)
  }

  test("4.json codec") {
    val a :: b :: c :: d :: e :: f :: g :: _ = guard
      .withRestartPolicy(policies.giveUp)
      .eventStream { gd =>
        gd.action("t", _.notice).withRetryPolicy(policy).delay(throw new Exception("oops")).run

      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ServiceStop])
  }

  test("5.should receive at least 3 report event") {
    val s :: b :: c :: d :: _ = guard
      .withMetricReport(policies.crontab(cron_1second))
      .eventStream(_.action("t", _.silent).retry(IO.never).run)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(d.isInstanceOf[MetricReport])
  }

  test("6.force reset") {
    val s :: b :: c :: _ = guard
      .withMetricReport(policies.crontab(cron_1second))
      .eventStream(ag => ag.metrics.reset >> ag.metrics.reset)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReset])
    assert(c.isInstanceOf[MetricReset])
  }

  test("7.normal service stop after two operations") {
    val Vector(s, a, b, c, d, e) = guard
      .eventStream(gd => gd.action("t", _.notice).delay(1).run >> gd.action("t", _.notice).retry(IO(2)).run)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionDone])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionDone])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("8.combine two event streams") {
    val guard = TaskGuard[IO]("two service")
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd =>
      gd.action("t", _.notice).retry(IO(1)).run >> gd.action("t", _.notice).retry(IO(2)).run)
    val ss2 = s2.eventStream(gd =>
      gd.action("t", _.notice).retry(IO(1)).run >> gd.action("t", _.notice).retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionDone]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStop]) == 2)
  }

  test("9.should give up") {

    val List(a, b, c, d, e, f, g) = guard
      .withRestartPolicy(policies.giveUp)
      .eventStream { gd =>
        gd.action("t", _.notice).withRetryPolicy(policy).retry(IO.raiseError(new Exception)).run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ServiceStop])
  }

  test("10.dummy agent should not block") {
    val dummy = TaskGuard.dummyAgent[IO]
    dummy.use(_.action("test", _.notice).retry(IO(1)).run.replicateA(3)).unsafeRunSync()
  }

  test("11.policy start over") {

    val p1     = policies.constant(1.seconds).limited(1)
    val p2     = policies.constant(2.seconds).limited(1)
    val p3     = policies.constant(3.seconds).limited(1)
    val policy = p1.followedBy(p2).followedBy(p3).repeat
    val List(a, b, c, d, e, f, g, h) = guard
      .withRestartPolicy(policy)
      .eventStream(_ => IO.raiseError(new Exception("oops")))
      .evalMapFilter[IO, Tick] {
        case sp: ServicePanic => IO(Some(sp.tick))
        case _                => IO(None)
      }
      .take(8)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.index == 1)
    assert(a.snooze == 1.second.toJava)
    assert(b.index == 2)
    assert(b.previous == a.wakeup)
    assert(b.snooze == 2.second.toJava)
    assert(c.index == 3)
    assert(c.previous == b.wakeup)
    assert(c.snooze == 3.second.toJava)

    assert(d.index == 4)
    assert(d.previous == c.wakeup)
    assert(d.snooze == 1.second.toJava)
    assert(e.index == 5)
    assert(e.previous == d.wakeup)
    assert(e.snooze == 2.second.toJava)
    assert(f.index == 6)
    assert(f.previous == e.wakeup)
    assert(f.snooze == 3.second.toJava)

    assert(g.index == 7)
    assert(g.previous == f.wakeup)
    assert(g.snooze == 1.second.toJava)
    assert(h.index == 8)
    assert(h.previous == g.wakeup)
    assert(h.snooze == 2.second.toJava)
  }

  test("12.policy threshold start over") {
    val List(a, b, c, d, e, f, g, h) = guard
      .withRestartPolicy(policies.fibonacci(1.seconds,4))
      .withMetricServer(identity)
      .eventStream(_ => IO.raiseError(new Exception("oops")))
      .evalMapFilter[IO, Tick] {
        case sp: ServicePanic => IO(Some(sp.tick))
        case _                => IO(None)
      }
      .debug()
      .take(8)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.index == 1)
    assert(a.snooze == 1.second.toJava)
    assert(b.index == 2)
    assert(b.previous == a.wakeup)
    assert(b.snooze == 1.second.toJava)
    assert(c.index == 3)
    assert(c.previous == b.wakeup)
    assert(c.snooze == 2.second.toJava)
    assert(d.index == 4)
    assert(d.previous == c.wakeup)
    assert(d.snooze == 3.second.toJava)

    assert(e.index == 5)
    assert(e.previous == d.wakeup)
    assert(e.snooze == 1.second.toJava)
    assert(f.index == 6)
    assert(f.previous == e.wakeup)
    assert(f.snooze == 1.second.toJava)
    assert(g.index == 7)
    assert(g.previous == f.wakeup)
    assert(g.snooze == 2.second.toJava)
    assert(h.index == 8)
    assert(h.previous == g.wakeup)
    assert(h.snooze == 3.second.toJava)
  }
}
