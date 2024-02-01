package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.jawn.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.ControlThrowable

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard")
      .updateConfig(_.withZoneId(singaporeTime))
      .service("retry test")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.seconds)))

  val policy: Policy = policies.fixedDelay(1.seconds).limited(3)

  test("1.retry - completed trivial") {
    val Vector(s, c) = serviceGuard.eventStream { gd =>
      gd.action("t").retry(fun3 _).logOutput((a, _) => a.asJson).withWorthRetry(_ => true).run((1, 1, 1))
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(c.isInstanceOf[ServiceStop])
  }

  test("2.retry - completed notice") {
    val Vector(s, a, b, c, d, e, f, g) = serviceGuard.eventStream { gd =>
      val ag =
        gd.action("t", _.bipartite.timed.counted)
          .retry(fun5 _)
          .logInput(_._3.asJson)
          .withWorthRetry(_ => true)
      List(1, 2, 3).traverse(i => ag.run((i, i, i, i, i)))
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionDone])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionDone])
    assert(e.isInstanceOf[ActionStart])
    assert(f.isInstanceOf[ActionDone])
    assert(g.isInstanceOf[ServiceStop])

    assert(a.asInstanceOf[ActionEvent].actionId == b.asInstanceOf[ActionEvent].actionId)
    assert(c.asInstanceOf[ActionEvent].actionId == d.asInstanceOf[ActionEvent].actionId)
    assert(e.asInstanceOf[ActionEvent].actionId == f.asInstanceOf[ActionEvent].actionId)

    assert(a.asInstanceOf[ActionEvent].actionId =!= c.asInstanceOf[ActionEvent].actionId)
    assert(a.asInstanceOf[ActionEvent].actionId =!= e.asInstanceOf[ActionEvent].actionId)
  }

  test("3.retry - all fail") {
    val policy = policies.fixedDelay(0.1.seconds).limited(1)
    val Vector(s, a, b, c, d, e, f, g, h, i, j) = serviceGuard.eventStream { gd =>
      val ag = gd
        .action("t", _.bipartite.counted.policy(policy))
        .retry((_: Int, _: Int, _: Int) => IO.raiseError[Int](new Exception))
        .logOutput((in, out) => (in._3, out).asJson)
        .logOutput((in, out) => (in, out).asJson)

      List(1, 2, 3).traverse(i => ag.run((i, i, i)).attempt)
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

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
    assert(ap.tick.zoneId == ap.serviceParams.taskParams.zoneId)

    assert(a.asInstanceOf[ActionEvent].actionId == b.asInstanceOf[ActionEvent].actionId)
    assert(a.asInstanceOf[ActionEvent].actionId == c.asInstanceOf[ActionEvent].actionId)
    assert(d.asInstanceOf[ActionEvent].actionId == e.asInstanceOf[ActionEvent].actionId)
    assert(d.asInstanceOf[ActionEvent].actionId == f.asInstanceOf[ActionEvent].actionId)
    assert(g.asInstanceOf[ActionEvent].actionId == h.asInstanceOf[ActionEvent].actionId)
    assert(g.asInstanceOf[ActionEvent].actionId == i.asInstanceOf[ActionEvent].actionId)
  }

  test("4.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd.action("t", _.bipartite.timed.policy(policy))
        .retry((_: Int) =>
          IO(if (i < 2) {
            i += 1; throw new Exception
          } else i))
        .logOutput((a, _) => a.asJson)
        .logInput(_.asJson)
        .logError((a, _) => a.asJson)
        .run(1)
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(a.asInstanceOf[ActionStart].notes.nonEmpty)
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionDone])
    assert(d.asInstanceOf[ActionDone].notes.nonEmpty)
    assert(e.isInstanceOf[ServiceStop])

    assert(a.asInstanceOf[ActionEvent].actionId == b.asInstanceOf[ActionEvent].actionId)
    assert(a.asInstanceOf[ActionEvent].actionId == c.asInstanceOf[ActionEvent].actionId)
    assert(a.asInstanceOf[ActionEvent].actionId == d.asInstanceOf[ActionEvent].actionId)
  }

  test("5.retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, d, e, f) = serviceGuard.eventStream { gd =>
      gd.action("t", _.critical.bipartite.policy(policy))
        .retry((_: Int) =>
          IO(if (i < 2) {
            i += 1
            throw new Exception
          } else i))
        .logInput(_.asJson)
        .run(1)
    }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionStart].notes.nonEmpty)
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionDone])
    assert(e.asInstanceOf[ActionDone].notes.isEmpty)
    assert(f.isInstanceOf[ServiceStop])
  }

  test("6.retry - should escalate to up level if retry failed") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { gd =>
        gd.action("t", _.counted.policy(policies.fixedDelay(1.seconds).limited(3)))
          .retry((_: Int) => IO.raiseError[Int](new Exception("oops")))
          .logInput(_.asJson)
          .run(1)
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
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
    val List(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream(ag =>
        ag.action("t", _.normal.timed.policy(policy))
          .retry(IO.raiseError[Int](new NullPointerException))
          .logOutput(_.asJson)
          .run)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .take(6)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(e.asInstanceOf[ActionFail].notes.isEmpty)
    assert(f.isInstanceOf[ServiceStop])
  }

  test("8.retry - isWorthRetry - should retry") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { gd =>
        gd.action("t", _.policy(policies.fixedDelay(0.1.seconds).limited(3)))
          .retry(IO.raiseError(MyException()))
          .withWorthRetry(_.isInstanceOf[MyException])
          .logErrorM(_ => IO(Json.fromString("worth-retry")))
          .run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
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
    val Vector(s, a, b, c) = serviceGuard
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { gd =>
        gd.zonedNow >>
          gd.action("t", _.bipartite.policy(policies.fixedDelay(0.1.seconds).limited(3)))
            .retry(IO.raiseError(new Exception))
            .withWorthRetryM(x => IO(x.isInstanceOf[MyException]))
            .logError(_ => Json.Null)
            .run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(2.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServicePanic])
  }

  test("10.retry - isWorthRetry - throw exception") {
    val Vector(s, a, b, c) = serviceGuard
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { gd =>
        gd.action("t", _.bipartite.policy(policies.fixedDelay(0.1.seconds).limited(3)))
          .retry(IO.raiseError(new Exception))
          .withWorthRetryM(_ => IO.raiseError[Boolean](new Exception()))
          .run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(2.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServicePanic])
  }

  test("11.quasi syntax") {
    serviceGuard.eventStream { ag =>
      val builder = ag.action("quasi", _.bipartite)
      builder.parQuasi(IO("a"), IO("b")).run >>
        builder.parQuasi(List(IO.raiseError(new Exception("a")), IO.raiseError(new Exception("b")))).run >>
        builder.parQuasi(8, List(IO("a"), IO.raiseError(new Exception("a")))).run >>
        builder.quasi(List(IO("a"), IO("b"))).run >>
        builder.quasi(IO("a"), IO.raiseError(new Exception)).run >>
        builder.quasi(IO.print("a"), IO.print("b")).run
    }.compile.drain.unsafeRunSync()
  }

  test("12.cron policy") {
    val List(a, b, c, d, e, f, g) = serviceGuard
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream(
        _.action("cron", _.bipartite.policy(policies.crontab(_.secondly).limited(3)))
          .retry(IO.raiseError(new Exception("oops")))
          .run)
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
    val err = IO.raiseError(new ControlThrowable("fatal error") {})
    val List(a, b, c, d, e, f, g, h, i) = serviceGuard("fatal")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { agent =>
        val a =
          agent.action("a", _.policy(policies.fixedDelay(1.seconds)).timed.counted.bipartite).retry(err).run
        val b =
          agent.action("b", _.policy(policies.fixedDelay(1.seconds)).timed.counted.unipartite).retry(err).run
        val c = agent.action("c", _.policy(policies.fixedDelay(1.seconds)).timed).retry(err).run
        val d = agent.action("d", _.policy(policies.fixedDelay(1.seconds)).counted).retry(err).run
        val e = agent.action("e", _.policy(policies.fixedDelay(1.seconds))).retry(err).run
        val f = agent.action("f", _.policy(policies.fixedDelay(1.seconds)).unipartite).retry(err).run
        agent.action("q").quasi(a, b, c, d, e, f).run
      }
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

  test("14.logError json exception") {
    val List(a, b, c, d) = serviceGuard
      .eventStream(agent =>
        agent
          .action("input error", _.bipartite)
          .retry((_: Int) => IO.raiseError[Int](new Exception))
          .logErrorM((_, _) => IO.raiseError[Json](new Exception))
          .run(1)
          .attempt)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(c.asInstanceOf[ActionFail].notes.nonEmpty)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("15.logError null") {
    val List(a, b, c) = TaskGuard[IO]("logError")
      .service("no exception")
      .eventStream(
        _.action("exception", _.unipartite.counted)
          .retry(IO.raiseError(new Exception))
          .logError(_ => null.asInstanceOf[String].asJson)
          .run)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ActionFail].notes.nonEmpty)
    assert(c.isInstanceOf[ServiceStop])
  }

  test("16.retry - aware") {
    val Vector(s, a, b, c, d) = serviceGuard.eventStream { gd =>
      val ag =
        gd.action("t", _.unipartite).retry(fun5 _).logInput(_._3.asJson).withWorthRetry(_ => true)
      List(1, 2, 3).traverse(i => ag.run((i, i, i, i, i)))
    }.evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionDone])
    assert(a.asInstanceOf[ActionDone].notes.isEmpty)
    assert(b.isInstanceOf[ActionDone])
    assert(c.isInstanceOf[ActionDone])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("17.retry - delay") {
    var k = 0
    def tt = if (k == 0) { k += 1; throw new Exception() }
    else { k += 1; 0 }
    serviceGuard.eventStream { agent =>
      agent.action("delay", _.bipartite.insignificant.policy(policies.fixedDelay(1.seconds))).delay(tt).run
    }.compile.drain.unsafeRunSync()
    assert(k == 2)
  }

  test("18.resource") {
    val List(a, b, c, d) = serviceGuard
      .eventStream(agent =>
        agent
          .action("resource", _.timed.counted)
          .retry((i: Int) => IO(i.toString))
          .asResource
          .use(_.run(1) >> agent.metrics.report) >> agent.metrics.report)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[MetricReport].snapshot.timers.nonEmpty)
    assert(c.asInstanceOf[MetricReport].snapshot.timers.isEmpty)
    assert(d.isInstanceOf[ServiceStop])
  }

  test("19.seq quasi") {
    serviceGuard.eventStream { agent =>
      agent.action("quasi.seq").quasi(IO(1), IO.raiseError(new Exception)).run
    }.compile.drain.unsafeRunSync()
  }

  test("20.par quasi") {
    serviceGuard.eventStream { agent =>
      agent.action("quasi.seq").parQuasi(2)(IO(1), IO(2)).run
    }.compile.drain.unsafeRunSync()
  }

  test("21.all fail quasi") {
    serviceGuard.eventStream { agent =>
      agent.action("quasi.seq").quasi(IO.raiseError(new Exception), IO.raiseError(new Exception)).run
    }.compile.drain.unsafeRunSync()
  }

  test("22.silent count") {
    val List(a, b, c) = TaskGuard[IO]("silent")
      .service("count")
      .eventStream(
        _.action("exception", _.silent.counted.policy(policies.fixedDelay(1.seconds)))
          .retry((_: Int) => IO.raiseError(new Exception))
          .withWorthRetry(_ => true)
          .logErrorM((_, _) => IO(Json.fromInt(1)))
          .run(1))
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
          .retry((_: Int) => IO.raiseError(new Exception))
          .withWorthRetry(_ => true)
          .logErrorM((_, _) => IO(Json.fromInt(1)))
          .run(1))
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
  }
}
