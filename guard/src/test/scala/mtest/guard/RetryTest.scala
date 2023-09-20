package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.console
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.Json
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.ControlThrowable

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard").service("retry test").withRestartPolicy(constant_1second)

  val policy: Policy = policies.constant(1.seconds).limited(3)

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
        gd.action("t", _.notice).retry(fun5 _).logInput(_._3.asJson).withWorthRetry(_ => true)
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
  }

  test("3.retry - all fail") {
    val policy = policies.constant(0.1.seconds).limited(1)
    val Vector(s, a, b, c, d, e, f, g, h, i, j) = serviceGuard.eventStream { gd =>
      val ag = gd
        .action("t", _.notice)
        .withRetryPolicy(policy)
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

  }

  test("4.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = serviceGuard.eventStream { gd =>
      gd.action("t", _.notice)
        .withRetryPolicy(policy)
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
  }

  test("5.retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, d, e, f) = serviceGuard.eventStream { gd =>
      gd.action("t", _.critical.notice)
        .withRetryPolicy(policy)
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
      .withRestartPolicy(policies.giveUp)
      .eventStream { gd =>
        gd.action("t")
          .withRetryPolicy(policies.constant(1.seconds).limited(3))
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
      .withRestartPolicy(policies.giveUp)
      .eventStream(ag =>
        ag.action("t")
          .withRetryPolicy(policy)
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
      .withRestartPolicy(policies.giveUp)
      .eventStream { gd =>
        gd.action("t")
          .withRetryPolicy(policies.constant(0.1.seconds).limited(3))
          .retry(IO.raiseError(MyException()))
          .withWorthRetryM(x => IO(x.isInstanceOf[MyException]))
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
      .withRestartPolicy(constant_1hour)
      .eventStream { gd =>
        gd.action("t", _.notice)
          .withRetryPolicy(policies.constant(0.1.seconds).limited(3))
          .retry(IO.raiseError(new Exception))
          .withWorthRetry(_.isInstanceOf[MyException])
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
      .withRestartPolicy(constant_1hour)
      .eventStream { gd =>
        gd.action("t", _.notice)
          .withRetryPolicy(policies.constant(0.1.seconds).limited(3))
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
      val builder = ag.action("quasi", _.notice)
      builder.parQuasi(IO("a"), IO("b")).run >>
        builder.parQuasi(List(IO("a"), IO("b"))).run >>
        builder.parQuasi(8, List(IO("a"), IO("b"))).run >>
        builder.quasi(List(IO("a"), IO("b"))).run >>
        builder.quasi(IO("a"), IO("b")).run >>
        builder.quasi(IO.print("a"), IO.print("b")).run
    }
  }

  test("12.cron policy") {
    val List(a, b, c, d, e, f, g) = serviceGuard
      .withRestartPolicy(policies.giveUp)
      .eventStream(
        _.action("cron", _.notice)
          .withRetryPolicy(policies.crontab(cron_1second, ZoneId.systemDefault()).limited(3))
          .retry(IO.raiseError(new Exception("oops")))
          .run)
      .evalTap(console.simple[IO])
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

  test("13.should not retry fatal error") {
    val List(a, b, c, d) = serviceGuard
      .withRestartPolicy(policies.giveUp)
      .eventStream(
        _.action("fatal", _.critical.trivial.normal.notice)
          .withRetryPolicy(constant_1second)
          .retry(IO.raiseError(new ControlThrowable("fatal error") {}))
          .run)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("14. logError json exception") {
    val List(a, b, c, d) = serviceGuard
      .eventStream(agent =>
        agent
          .action("input error", _.notice)
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

  test("15. logError null") {
    val List(a, b, c) = TaskGuard[IO]("logError")
      .service("no exception")
      .eventStream(
        _.action("exception", _.aware)
          .retry(IO.raiseError(new Exception))
          .logError(_ => null.asInstanceOf[String].asJson)
          .run)
      .debug()
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
        gd.action("t", _.aware).retry(fun5 _).logInput(_._3.asJson).withWorthRetry(_ => true)
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
      agent.action("delay", _.notice.insignificant).withRetryPolicy(constant_1second).delay(tt).run
    }.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
    assert(k == 2)
  }

}
