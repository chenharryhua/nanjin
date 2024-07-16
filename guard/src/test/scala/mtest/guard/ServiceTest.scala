package mtest.guard

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, Policy, Tick}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import cron4s.Cron
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.ControlThrowable

class ServiceTest extends AnyFunSuite {

  val guard: TaskGuard[IO] = TaskGuard[IO]("service-level-guard").updateConfig(
    _.withHomePage("https://abc.com/efg")
      .withZoneId(londonTime)
      .withRestartPolicy(Policy.fixedDelay(1.seconds))
      .addBrief(Json.fromString("test")))

  val policy: Policy = Policy.fixedDelay(0.1.seconds).limited(3)

  test("1.should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .service("normal")
      .updateConfig(
        _.withRestartPolicy(Policy.fixedDelay(3.seconds))
          .withMetricReport(Policy.crontab(_.hourly))
          .withMetricDailyReset
          .withHttpServer(identity))
      .eventStream(gd =>
        gd.action("t", _.silent)
          .delay(1)
          .buildWith(_.tapOutput((_, _) => null))
          .use(_.run(()))
          .delayBy(1.second))
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(d.isInstanceOf[ServiceStop])
    assert(d.asInstanceOf[ServiceStop].cause.exitCode == 0)
    val ss = a.asInstanceOf[ServiceStart]
    assert(ss.tick.sequenceId == ss.serviceParams.serviceId)
    assert(ss.tick.zoneId == londonTime)

  }

  test("2.escalate to up level if retry failed") {
    val Vector(s, a, b, c, d, e, f) = guard
      .service("escalate")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(0.second).jitter(30.minutes, 50.minutes)))
      .eventStream { gd =>
        gd.action("t", _.bipartite.policy(policy))
          .retry(IO.raiseError[Int](new Exception(gd.toZonedDateTime(Instant.now()).toString)))
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
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
    val sp = f.asInstanceOf[ServicePanic]
    assert(sp.tick.sequenceId == sp.serviceParams.serviceId)
    assert(sp.tick.zoneId == sp.serviceParams.zoneId)
  }

  ignore("3.should stop when fatal error occurs") {
    val List(a, b, c, d) = guard
      .service("stop")
      .updateConfig(_.withRestartPolicy(Policy.crontab(Cron.unsafeParse("0-59 * * ? * *"))))
      .eventStream { gd =>
        gd.action("fatal error", _.bipartite.policy(policy))
          .retry(IO.raiseError[Int](new ControlThrowable("fatal error") {}))
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
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
      .service("json")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.action("t", _.bipartite.policy(policy))
          .delay[Int](throw new Exception("oops"))
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
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
      .service("report")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(_.action("t", _.silent).retry(IO.never[Int]).buildWith(identity).use(_.run(())))
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.second)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(d.isInstanceOf[MetricReport])
    val mr = d.asInstanceOf[MetricReport]
    assert(mr.index.asInstanceOf[MetricIndex.Periodic].tick.sequenceId == mr.serviceParams.serviceId)
  }

  test("6.force reset") {
    val s :: b :: c :: _ = guard
      .service("reset")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(ag => ag.metrics.reset >> ag.metrics.reset)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReset])
    assert(c.isInstanceOf[MetricReset])
  }

  test("7.normal service stop after two operations") {
    val Vector(s, a, b, c, d, e) = guard
      .service("two")
      .eventStream(gd =>
        gd.action("t", _.bipartite).delay(1).buildWith(identity).use(_.run(())) >>
          gd.action("t", _.bipartite).retry(IO(2)).buildWith(identity).use(_.run(())))
      .map(checkJson)
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
      gd.action("t", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(())) >> gd
        .action("t", _.bipartite)
        .retry(IO(2))
        .buildWith(identity)
        .use(_.run(())))
    val ss2 = s2.eventStream(gd =>
      gd.action("t", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(())) >> gd
        .action("t", _.bipartite)
        .retry(IO(2))
        .buildWith(identity)
        .use(_.run(())))

    val vector = ss1.merge(ss2).map(checkJson).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionDone]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStop]) == 2)
  }

  test("9.should give up") {

    val List(a, b, c, d, e, f, g) = guard
      .service("give up")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.action("t", _.bipartite.policy(policy))
          .retry(IO.raiseError[Int](new Exception))
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
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

  test("10.should stop after 2 panic") {

    val List(a, b, c, d, e, f, g, h, i) = guard
      .service("panic")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.seconds).limited(2)))
      .eventStream(_.action("t").retry(IO.raiseError[Int](new Exception)).buildWith(identity).use(_.run(())))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.isInstanceOf[ActionFail])
    assert(i.isInstanceOf[ServiceStop])
  }

  test("11.policy start over") {

    val p1     = Policy.fixedDelay(1.seconds).limited(1)
    val p2     = Policy.fixedDelay(2.seconds).limited(1)
    val p3     = Policy.fixedDelay(3.seconds).limited(1)
    val policy = p1.followedBy(p2).followedBy(p3).repeat
    println(policy.show)
    val List(a, b, c, d, e, f, g, h) = guard
      .service("start over")
      .updateConfig(_.withRestartPolicy(policy))
      .eventStream(_ => IO.raiseError[Int](new Exception("oops")))
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

  test("12.policy threshold start over") {

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
              box.getAndUpdate(_ + 1.second).flatMap(IO.sleep) >>
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

  test("13.service config") {
    TaskGuard[IO]("abc")
      .service("abc")
      .updateConfig(
        _.withRestartPolicy(Policy.fixedDelay(1.second))
          .withMetricReset(Policy.giveUp)
          .withMetricReport(Policy.crontab(crontabs.secondly))
          .withMetricDailyReset
          .withRestartThreshold(2.second))
      .eventStream(_ => IO(()))
      .map(checkJson)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("14.throw exception in construction") {
    val List(a, b) = guard
      .service("simple")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { _ =>
        val c        = true
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
}
