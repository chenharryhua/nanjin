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
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
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
    val Vector(a, d) = guard
      .service("normal")
      .updateConfig(
        _.withRestartPolicy(Policy.fixedDelay(3.seconds))
          .withMetricReport(Policy.crontab(_.hourly))
          .withMetricDailyReset
          .withHttpServer(identity))
      .eventStream(gd =>
        gd.facilitate("t")(_.action(IO(1)).buildWith(identity)).use(_.run(())).delayBy(1.second))
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
    val Vector(s, a, b, c, d) = guard
      .service("escalate")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(0.second).jitter(30.minutes, 50.minutes)))
      .eventStream { gd =>
        gd.facilitate("t", _.withPolicy(policy))(_.action(
          IO.raiseError[Int](new Exception(gd.toZonedDateTime(Instant.now()).toString))).build).use(_.run(()))
      }
      .map(checkJson)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ServiceMessage])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServicePanic])
    val sp = d.asInstanceOf[ServicePanic]
    assert(sp.tick.sequenceId == sp.serviceParams.serviceId)
    assert(sp.tick.zoneId == sp.serviceParams.zoneId)
  }

  test("3.should receive at least 3 report event") {
    val s :: b :: c :: d :: _ = guard
      .service("report")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(_.facilitate("t")(_.action(IO.never[Int]).buildWith(identity)).use(_.run(())))
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

  test("4.force reset") {
    val s :: b :: c :: _ = guard
      .service("reset")
      .updateConfig(_.withMetricReport(Policy.crontab(_.secondly)))
      .eventStream(ag => ag.adhoc.reset >> ag.adhoc.reset)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReset])
    assert(c.isInstanceOf[MetricReset])
  }

  test("7.should give up") {
    val List(a, b, c, d, e) = guard
      .service("give up")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.facilitate("t", _.withPolicy(policy))(
          _.action(IO.raiseError[Int](new Exception)).buildWith(identity)).use(_.run(()))
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("8.should stop after 2 panic") {

    val List(a, b, c, d, e, f, g, h, i) = guard
      .service("panic")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.seconds).limited(2)))
      .eventStream(_.facilitate("t", _.withPolicy(Policy.fixedDelay(1.seconds).limited(1)))(
        _.action(IO.raiseError[Int](new Exception)).buildWith(identity)).use(_.run(())))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.isInstanceOf[ServiceMessage])
    assert(i.isInstanceOf[ServiceStop])
  }

  test("9.policy start over") {

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

  test("10.policy threshold start over") {

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

  test("11.service config") {
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

  test("12.throw exception in construction") {
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

  test("13.out of memory -- happy failure") {
    val run = guard
      .service("out of memory")
      .eventStream(_.facilitate("oom", _.withPolicy(Policy.fixedDelay(1.seconds).limited(3)))(
        _.action(IO.raiseError[Int](new OutOfMemoryError())).build).use(_(())))
      .map(checkJson)
      .compile
      .drain
    assertThrows[OutOfMemoryError](run.unsafeRunSync())
  }
}
