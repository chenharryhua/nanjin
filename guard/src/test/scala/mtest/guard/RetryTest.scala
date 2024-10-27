package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.common.chrono.zones.singaporeTime
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.control.ControlThrowable

final case class MyException() extends Exception("my exception")

class RetryTest extends AnyFunSuite {

  private val task: TaskGuard[IO] =
    TaskGuard[IO]("retry-guard")
      .updateConfig(_.withZoneId(singaporeTime))
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.seconds)))

  val policy: Policy = Policy.fixedDelay(1.seconds).limited(3)

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
          gd.action("t")
            .retry(fun5 _)
            .buildWith(_.tapInput(_._3.asJson).worthRetry(_ => true).withPublishStrategy(_.Bipartite))
        List(1, 2, 3).traverse(i => ag.use(_.run((i, i, i, i, i))))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ServiceMessage])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceMessage])
    assert(g.isInstanceOf[ServiceStop])

  }

  test("3.retry - all fail") {
    val policy = Policy.fixedDelay(0.1.seconds).limited(1)
    val Vector(s, a, b, c, d, e, f, g, h, i, j) = task
      .service("all fail")
      .eventStream { gd =>
        val ag = gd
          .action("t")
          .retry((_: Int, _: Int, _: Int) => IO.raiseError[Int](new Exception))
          .buildWith(
            _.tapOutput((in, out) => (in._3, out).asJson).withPolicy(policy).withPublishStrategy(_.Bipartite))

        List(1, 2, 3).traverse(i => ag.use(_.run((i, i, i)).attempt))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ServiceMessage])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceMessage])
    assert(g.isInstanceOf[ServiceMessage])
    assert(h.isInstanceOf[ServiceMessage])
    assert(i.isInstanceOf[ServiceMessage])
    assert(j.isInstanceOf[ServiceStop])

  }

  test("4.retry - should retry 2 times when operation fail") {
    var i = 0
    val Vector(s, a, b, c, d, e) = task
      .service("2 times")
      .eventStream { gd =>
        gd.action("t")
          .retry((_: Int) =>
            IO(if (i < 2) {
              i += 1; throw new Exception
            } else i))
          .buildWith(_.tapOutput((a, _) => a.asJson)
            .tapInput(_.asJson)
            .withPublishStrategy(_.Bipartite)
            .withPolicy(policy))
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ServiceMessage])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("5.retry - should retry 2 times when operation fail - low") {
    var i = 0
    val Vector(s, b, c, d, e, f) = task
      .service("low")
      .eventStream { gd =>
        gd.action("t")
          .retry((_: Int) =>
            IO(if (i < 2) {
              i += 1
              throw new Exception
            } else i))
          .buildWith(_.tapInput(_.asJson).withPolicy(policy).withPublishStrategy(_.Bipartite))
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("6.retry - should escalate to up level if retry failed") {
    val Vector(s, b, c, d, e, f) = task
      .service("escalate")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.action("t")
          .retry((_: Int) => IO.raiseError[Int](new Exception("oops")))
          .buildWith(_.tapInput(_.asJson).withPolicy(Policy.fixedDelay(1.seconds).limited(3)))
          .use(_.run(1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceStop])

  }

  test("7.retry - Null pointer exception") {
    val List(a, b, c, d, e, f) = task
      .service("null exception")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream(ag =>
        ag.action("t")
          .retry(IO.raiseError[Int](new NullPointerException))
          .buildWith(_.tapOutput((_, a) => a.asJson).withPolicy(policy))
          .use(_.run(())))
      .map(checkJson)
      .take(6)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("8.retry - isWorthRetry - should retry") {
    val Vector(s, b, c, d, e, f) = task
      .service("retry")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { gd =>
        gd.action("t")
          .retry(IO.raiseError[Int](MyException()))
          .buildWith(
            _.worthRetry(_.isInstanceOf[MyException]).withPolicy(Policy.fixedDelay(0.1.seconds).limited(3)))
          .use(_.run(()))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceStop])
  }

  test("9.retry - isWorthRetry - should not retry") {
    val Vector(s, a, b, c) = task
      .service("no retry")
      .updateConfig(_.withRestartPolicy(Policy.fixedDelay(1.hour)))
      .eventStream { gd =>
        gd.zonedNow >>
          gd.action("t")
            .retry(IO.raiseError[Int](new Exception))
            .buildWith(_.worthRetry(x => x.isInstanceOf[MyException])
              .withPolicy(Policy.fixedDelay(0.1.seconds).limited(3))
              .withPublishStrategy(_.Bipartite))
            .use(_.run(()))
      }
      // .debug()
      .map(checkJson)
      .interruptAfter(2.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ServiceMessage])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServicePanic])
  }

  test("12.cron policy") {
    val List(a, b, c, d, e, f, g) = task
      .service("cron")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream(
        _.action("cron")
          .retry(IO.raiseError[Int](new Exception("oops")))
          .buildWith(_.withPolicy(Policy.crontab(_.secondly).limited(3)).withPublishStrategy(_.Bipartite))
          .use(_.run(())))
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceMessage])
    assert(g.isInstanceOf[ServiceStop])

  }

  ignore("13.should not retry fatal error") {
    val err = IO.raiseError[Int](new ControlThrowable("fatal error") {})
    val List(a, b, c, d, e, f, g, h, i) = task
      .service("fatal")
      .updateConfig(_.withRestartPolicy(Policy.giveUp))
      .eventStream { agent =>
        val a =
          agent
            .action("a")
            .retry(err)
            .buildWith(_.withPolicy(Policy.fixedDelay(1.seconds)).withPublishStrategy(_.Bipartite))
            .use(_.run(()))
        val b =
          agent
            .action("b")
            .retry(err)
            .buildWith(_.withPolicy(Policy.fixedDelay(1.seconds)).withPublishStrategy(_.Unipartite))
            .use(_.run(()))
        val c =
          agent.action("c").retry(err).buildWith(_.withPolicy(Policy.fixedDelay(1.seconds))).use(_.run(()))
        val d =
          agent.action("d").retry(err).buildWith(_.withPolicy(Policy.fixedDelay(1.seconds))).use(_.run(()))
        val e =
          agent.action("e").retry(err).buildWith(_.withPolicy(Policy.fixedDelay(1.seconds))).use(_.run(()))
        val f = agent
          .action("f")
          .retry(err)
          .buildWith(_.withPolicy(Policy.fixedDelay(1.seconds)).withPublishStrategy(_.Bipartite))
          .use(_.run(()))
        a >> b >> c >> d >> e >> f
      }
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
    assert(d.isInstanceOf[ServiceMessage])
    assert(e.isInstanceOf[ServiceMessage])
    assert(f.isInstanceOf[ServiceMessage])
    assert(g.isInstanceOf[ServiceMessage])
    assert(h.isInstanceOf[ServiceMessage])
    assert(i.isInstanceOf[ServiceStop])
  }

  test("16.retry - aware") {
    val Vector(s, a, b, c, d) = task
      .service("aware")
      .eventStream { gd =>
        val ag =
          gd.action("t")
            .retry(fun5 _)
            .buildWith(_.tapInput(_._3.asJson).worthRetry(_ => true).withPublishStrategy(_.Unipartite))
        List(1, 2, 3).traverse(i => ag.use(_.run((i, i, i, i, i))))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ServiceMessage])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
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
          .action("delay")
          .delay(tt)
          .buildWith(_.withPolicy(Policy.fixedDelay(1.seconds)).withPublishStrategy(_.Bipartite))
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
          .action("resource")
          .retry((i: Int) => IO(i.toString))
          .buildWith(identity)
          .use(_.run(1) >> agent.adhoc.report) >> agent.adhoc.report)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("22.silent count") {
    val List(a, b, c) = TaskGuard[IO]("silent")
      .service("count")
      .eventStream(
        _.action("exception")
          .retry((_: Int) => IO.raiseError[Int](new Exception))
          .buildWith(_.withPolicy(Policy.fixedDelay(1.seconds)))
          .use(_.run(1)))
      .map(checkJson)
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
  }

  test("23.unipartite time") {
    val List(a, b, c) = TaskGuard[IO]("unipartite")
      .service("time")
      .eventStream(
        _.action("exception")
          .retry((_: Int) => IO.raiseError[Int](new Exception))
          .buildWith(_.withPolicy(Policy.fixedDelay(1.seconds)).withPublishStrategy(_.Bipartite))
          .use(_.run(1)))
      .map(checkJson)
      .take(3)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ServiceMessage])
    assert(c.isInstanceOf[ServiceMessage])
  }
}
