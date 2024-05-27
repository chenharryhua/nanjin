package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import io.circe.Json
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.ControlThrowable

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  private val task: TaskGuard[IO] =
    TaskGuard[IO]("retry-guard")
      .updateConfig(_.withZoneId(singaporeTime))
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.seconds)))

  val policy: Policy = policies.fixedDelay(1.seconds).limited(3)

  test("1.retry - completed trivial") {
    val Vector(s, c) = task
      .service("trivial")
      .eventStream { gd =>
        gd.action("t")
          .retry(fun3 _)
          .buildWith(_.tapOutput((a, _) => a.asJson).worthRetry(_ => true))
          .use(_.run((1, 1, 1)))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("2.retry - completed notice") {
    val Vector(s, a, b, c, d, e, f, g) = task
      .service("notice")
      .eventStream { gd =>
        val ag =
          gd.action("t", _.bipartite.timed.counted)
            .retry(fun5 _)
            .buildWith(_.tapInput(_._3.asJson).worthRetry(_ => true))
        List(1, 2, 3).traverse(i => ag.use(_.run((i, i, i, i, i))))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionDone])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionDone])
    assert(e.isInstanceOf[ActionStart])
    assert(f.isInstanceOf[ActionDone])
    assert(g.isInstanceOf[ServiceStop])

  }

  test("3.retry - all fail") {
    val policy = policies.fixedDelay(0.1.seconds).limited(1)
    val Vector(s, a, b, c, d, e, f, g, h, i, j) = task
      .service("all fail")
      .eventStream { gd =>
        val ag = gd
          .action("t", _.bipartite.counted.policy(policy))
          .retry((_: Int, _: Int, _: Int) => IO.raiseError[Int](new Exception))
          .buildWith(_.tapOutput((in, out) => (in._3, out).asJson))

        List(1, 2, 3).traverse(i => ag.use(_.run((i, i, i)).attempt))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ActionStart])
    assert(h.isInstanceOf[ActionRetry])
    assert(i.isInstanceOf[ActionFail])
    assert(j.isInstanceOf[ServiceStop])

    val ap = h.asInstanceOf[ActionRetry]
    assert(ap.tick.sequenceId == ap.actionParams.serviceParams.serviceId)
    assert(ap.tick.zoneId == ap.serviceParams.zoneId)
  }

  test("4.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = task
      .service("2 times")
      .eventStream { gd =>
        gd.action("t", _.bipartite.timed.policy(policy))
          .retry((_: Int) =>
            IO(if (i < 2) {
              i += 1; throw new Exception
            } else i))
          .buildWith(_.tapOutput((a, _) => a.asJson).tapInput(_.asJson).tapError(_.asJson))
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionDone])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, d, e, f) = task
      .service("low")
      .eventStream { gd =>
        gd.action("t", _.critical.bipartite.policy(policy))
          .retry((_: Int) =>
            IO(if (i < 2) {
              i += 1
              throw new Exception
            } else i))
          .buildWith(_.tapInput(_.asJson))
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionDone])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("6.retry - should escalate to up level if retry failed") {
    val Vector(s, b, c, d, e, f) = task
      .service("escalate")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { gd =>
        gd.action("t", _.counted.policy(policies.fixedDelay(1.seconds).limited(3)))
          .retry((_: Int) => IO.raiseError[Int](new Exception("oops")))
          .buildWith(_.tapInput(_.asJson))
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServiceStop])

    assert(b.asInstanceOf[ActionRetry].tick.index == 1)
    assert(c.asInstanceOf[ActionRetry].tick.index == 2)
    assert(d.asInstanceOf[ActionRetry].tick.index == 3)
  }

  test("7.retry - Null pointer exception") {
    val List(a, b, c, d, e, f) = task
      .service("null exception")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream(ag =>
        ag.action("t", _.normal.timed.policy(policy))
          .retry(IO.raiseError[Int](new NullPointerException))
          .buildWith(_.tapOutput((_, a) => a.asJson))
          .use(_.run(())))
      .map(checkJson)
      .take(6)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("8.retry - isWorthRetry - should retry") {
    val Vector(s, b, c, d, e, f) = task
      .service("retry")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { gd =>
        gd.action("t", _.policy(policies.fixedDelay(0.1.seconds).limited(3)))
          .retry(IO.raiseError[Int](MyException()))
          .buildWith(_.worthRetry(_.isInstanceOf[MyException]).tapError(_ => Json.fromString("worth-retry")))
          .use(_.run(()))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("9.retry - isWorthRetry - should not retry") {
    val Vector(s, a, b, c) = task
      .service("no retry")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { gd =>
        gd.zonedNow >>
          gd.action("t", _.bipartite.policy(policies.fixedDelay(0.1.seconds).limited(3)))
            .retry(IO.raiseError[Int](new Exception))
            .buildWith(_.worthRetry(x => x.isInstanceOf[MyException]).tapError(_ => Json.Null))
            .use(_.run(()))
      }
      // .debug()
      .map(checkJson)
      .interruptAfter(2.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServicePanic])
  }

  test("12.cron policy") {
    val List(a, b, c, d, e, f, g) = task
      .service("cron")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream(
        _.action("cron", _.bipartite.policy(policies.crontab(_.secondly).limited(3)))
          .retry(IO.raiseError[Int](new Exception("oops")))
          .buildWith(identity)
          .use(_.run(())))
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

    val t1 = c.asInstanceOf[ActionRetry].tick
    val t2 = d.asInstanceOf[ActionRetry].tick
    val t3 = e.asInstanceOf[ActionRetry].tick

    assert(t2.previous == t1.wakeup)
    assert(t3.previous == t2.wakeup)
    assert(t1.snooze.toScala < 1.seconds)
    assert(t2.snooze.toScala < 1.seconds)
    assert(t3.snooze.toScala < 1.seconds)

  }

  ignore("13.should not retry fatal error") {
    val err = IO.raiseError[Int](new ControlThrowable("fatal error") {})
    val List(a, b, c, d, e, f, g, h, i) = task
      .service("fatal")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { agent =>
        val a =
          agent
            .action("a", _.policy(policies.fixedDelay(1.seconds)).timed.counted.bipartite)
            .retry(err)
            .buildWith(identity)
            .use(_.run(()))
        val b =
          agent
            .action("b", _.policy(policies.fixedDelay(1.seconds)).timed.counted.unipartite)
            .retry(err)
            .buildWith(identity)
            .use(_.run(()))
        val c =
          agent
            .action("c", _.policy(policies.fixedDelay(1.seconds)).timed)
            .retry(err)
            .buildWith(identity)
            .use(_.run(()))
        val d =
          agent
            .action("d", _.policy(policies.fixedDelay(1.seconds)).counted)
            .retry(err)
            .buildWith(identity)
            .use(_.run(()))
        val e = agent
          .action("e", _.policy(policies.fixedDelay(1.seconds)))
          .retry(err)
          .buildWith(identity)
          .use(_.run(()))
        val f = agent
          .action("f", _.policy(policies.fixedDelay(1.seconds)).unipartite)
          .retry(err)
          .buildWith(identity)
          .use(_.run(()))
        a >> b >> c >> d >> e >> f
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ActionFail])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ActionFail])
    assert(h.isInstanceOf[ActionFail])
    assert(i.isInstanceOf[ServiceStop])
  }

  test("16.retry - aware") {
    val Vector(s, a, b, c, d) = task
      .service("aware")
      .eventStream { gd =>
        val ag =
          gd.action("t", _.unipartite).retry(fun5 _).buildWith(_.tapInput(_._3.asJson).worthRetry(_ => true))
        List(1, 2, 3).traverse(i => ag.use(_.run((i, i, i, i, i))))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionDone])
    assert(b.isInstanceOf[ActionDone])
    assert(c.isInstanceOf[ActionDone])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("17.retry - delay") {
    var k = 0
    def tt = if (k == 0) { k += 1; throw new Exception() }
    else { k += 1; 0 }
    task
      .service("delay")
      .eventStream { agent =>
        agent
          .action("delay", _.bipartite.insignificant.policy(policies.fixedDelay(1.seconds)))
          .delay(tt)
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
      .compile
      .drain
      .unsafeRunSync()
    assert(k == 2)
  }

  test("18.resource") {
    val List(a, b, c, d) = task
      .service("resource")
      .eventStream(agent =>
        agent
          .action("resource", _.timed.counted)
          .retry((i: Int) => IO(i.toString))
          .buildWith(identity)
          .use(_.run(1) >> agent.metrics.report) >> agent.metrics.report)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[MetricReport].snapshot.timers.nonEmpty)
    assert(c.asInstanceOf[MetricReport].snapshot.timers.isEmpty)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("22.silent count") {
    val List(a, b, c) = TaskGuard[IO]("silent")
      .service("count")
      .eventStream(
        _.action("exception", _.silent.counted.policy(policies.fixedDelay(1.seconds)))
          .retry((_: Int) => IO.raiseError[Int](new Exception))
          .buildWith(identity)
          .use(_.run(1)))
      .map(checkJson)
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
  }

  test("23.unipartite time") {
    val List(a, b, c) = TaskGuard[IO]("unipartite")
      .service("time")
      .eventStream(
        _.action("exception", _.unipartite.timed.policy(policies.fixedDelay(1.seconds)))
          .retry((_: Int) => IO.raiseError[Int](new Exception))
          .buildWith(identity)
          .use(_.run(1)))
      .map(checkJson)
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
  }
}
