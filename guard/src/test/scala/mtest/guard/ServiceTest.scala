package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toShow
import com.comcast.ip4s.IpLiteralSyntax
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.common.chrono.{crontabs, policies, Policy, Tick}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import cron4s.Cron
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.*
import org.http4s.ember.client.EmberClientBuilder
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps
import scala.util.control.ControlThrowable

class ServiceTest extends AnyFunSuite {

  val guard: ServiceGuard[IO] = TaskGuard[IO]("service-level-guard")
    .updateConfig(_.withHomePage("https://abc.com/efg").withZoneId(londonTime))
    .service("service")
    .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.seconds)))
    .withBrief(Json.fromString("test"))

  val policy: Policy = policies.fixedDelay(0.1.seconds).limited(3)

  test("1.should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .updateConfig(
        _.withRestartPolicy(policies.fixedDelay(3.seconds))
          .withMetricReport(policies.crontab(_.hourly))
          .withMetricDailyReset)
      .withHttpServer(identity)
      .eventStream(gd => gd.action("t", _.silent).delay(1).logOutput(_ => null).run.delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(d.isInstanceOf[ServiceStop])
    assert(d.asInstanceOf[ServiceStop].cause.exitCode == 0)
    val ss = a.asInstanceOf[ServiceStart]
    assert(ss.tick.sequenceId == ss.serviceParams.serviceId)
    assert(ss.tick.zoneId == ss.serviceParams.taskParams.zoneId)

  }

  test("2.escalate to up level if retry failed") {
    val Vector(s, a, b, c, d, e, f) = guard
      .updateConfig(_.withRestartPolicy(policies.jitter(30.minutes, 50.minutes)))
      .eventStream { gd =>
        gd.action("t", _.bipartite.policy(policy))
          .retry(IO.raiseError(new Exception(gd.toZonedDateTime(Instant.now()).toString)))
          .run
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
    val sp = f.asInstanceOf[ServicePanic]
    assert(sp.tick.sequenceId == sp.serviceParams.serviceId)
    assert(sp.tick.zoneId == sp.serviceParams.taskParams.zoneId)
  }

  ignore("3.should stop when fatal error occurs") {
    val List(a, b, c, d) = guard
      .updateConfig(_.withRestartPolicy(policies.crontab(Cron.unsafeParse("0-59 * * ? * *"))))
      .eventStream { gd =>
        gd.action("fatal error", _.bipartite.policy(policy))
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
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { gd =>
        gd.action("t", _.bipartite.policy(policy)).delay(throw new Exception("oops")).run
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
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
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
    val mr = d.asInstanceOf[MetricReport]
    assert(mr.index.asInstanceOf[MetricIndex.Periodic].tick.sequenceId == mr.serviceParams.serviceId)
  }

  test("6.force reset") {
    val s :: b :: c :: _ = guard
      .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
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
      .eventStream(gd =>
        gd.action("t", _.bipartite).delay(1).run >> gd.action("t", _.bipartite).retry(IO(2)).run)
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
      gd.action("t", _.bipartite).retry(IO(1)).run >> gd.action("t", _.bipartite).retry(IO(2)).run)
    val ss2 = s2.eventStream(gd =>
      gd.action("t", _.bipartite).retry(IO(1)).run >> gd.action("t", _.bipartite).retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionDone]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStop]) == 2)
  }

  test("9.should give up") {

    val List(a, b, c, d, e, f, g) = guard
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { gd =>
        gd.withMeasurement("measurement")
          .action("t", _.bipartite.policy(policy))
          .retry(IO.raiseError(new Exception))
          .run
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

  test("10.should stop after 2 panic") {

    val List(a, b, c, d, e, f, g, h, i) = guard
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.seconds).limited(2)))
      .eventStream(_.action("t").retry(IO.raiseError(new Exception)).run)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .evalTap(console.json[IO])
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

    val p1     = policies.fixedDelay(1.seconds).limited(1)
    val p2     = policies.fixedDelay(2.seconds).limited(1)
    val p3     = policies.fixedDelay(3.seconds).limited(1)
    val policy = p1.followedBy(p2).followedBy(p3).repeat
    println(policy.show)
    val List(a, b, c, d, e, f, g, h) = guard
      .updateConfig(_.withRestartPolicy(policy))
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
    val policy: Policy = policies.fixedDelay(1.seconds, 2.seconds, 3.seconds, 4.seconds, 5.seconds)
    println(policy)
    val List(a, b, c, d, e, f, g, h) = guard
      .updateConfig(_.withRestartPolicy(policy))
      .updateConfig(_.withRestartThreshold(3.seconds))
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

  test("13.stop service") {
    val client = EmberClientBuilder
      .default[IO]
      .build
      .use { c =>
        c.expect[String]("http://localhost:9999/metrics") >>
          c.expect[String]("http://localhost:9999/metrics/vanilla") >>
          c.expect[String]("http://localhost:9999/metrics/yaml") >>
          c.expect[String]("http://localhost:9999/service") >>
          c.expect[String]("http://localhost:9999/service/stop")
      }
      .delayBy(3.seconds)

    val res =
      guard
        .updateConfig(_.withMetricReport(policies.crontab(_.secondly)))
        .withHttpServer(_.withPort(port"9999"))
        .eventStream { ag =>
          val ag1 = ag.withMeasurement("agent-1")
          val ag2 = ag.withMeasurement("agent-2")
          ag1.action("a", _.bipartite.timed.counted).retry(IO(())).run >>
            ag1.action("a2", _.bipartite.timed.counted).retry(IO(())).run >>
            ag1.gauge("g").timed.use_ >>
            ag1.gauge("g2").timed.use_ >>
            ag1.counter("c").inc(1) >>
            ag1.counter("c2").inc(1) >>
            ag2.histogram("h", _.BYTES).update(1) >>
            ag2.histogram("h2", _.BYTES).update(1) >>
            ag2.meter("m", _.MEGABYTES).mark(1) >>
            ag2.meter("m2", _.MEGABYTES).mark(2) >>
            ag2.metrics.report >>
            IO.sleep(10.hours)
        }
        .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
        .evalTap(console.simple[IO])
        .compile
        .toList <& client
    res.unsafeRunSync()
  }

  test("14.service config") {
    TaskGuard[IO]("abc")
      .service("abc")
      .updateConfig(
        _.withRestartPolicy(policies.fixedDelay(1.second))
          .withMetricReset(policies.giveUp)
          .withMetricReport(policies.crontab(crontabs.secondly))
          .withMetricDailyReset
          .withRestartThreshold(2.second))
      .eventStream(_ => IO(()))
      .compile
      .drain
      .unsafeRunSync()
  }
}
