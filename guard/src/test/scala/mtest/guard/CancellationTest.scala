package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.chrono.{policies, Policy}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class CancellationTest extends AnyFunSuite {

  val task: TaskGuard[IO] =
    TaskGuard[IO]("cancellation").updateConfig(
      _.disableJmx.disableHttpServer
        .withZoneId(sydneyTime)
        .withMetricDailyReset
        .withRestartPolicy(policies.fixedDelay(1.seconds)))

  val policy: Policy = policies.crontab(_.secondly).limited(3)

  test("1.cancellation - canceled actions are failed actions") {
    val Vector(a, b, c, d) = task
      .service("failed")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream(ag =>
        ag.action("canceled", _.bipartite.policy(policy))
          .retry(IO(1) <* IO.canceled)
          .buildWith(identity)
          .use(_.run(())))
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ServiceStop])
    assert(d.asInstanceOf[ServiceStop].cause.isInstanceOf[ServiceStopCause.ByCancellation.type])

  }

  test("2.cancellation - can be canceled externally") {
    val Vector(s, b, c) = task
      .service("externally")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { ag =>
        val a1 = ag.action("never", _.silent).retry(never_fun).buildWith(identity).use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServiceStop])
    assert(c.asInstanceOf[ServiceStop].cause.exitCode == 2)
  }

  test("3.canceled by external exception") {
    val Vector(s, b, c) = task
      .service("external exception")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { ag =>
        val a1 = ag.action("never").retry(never_fun).buildWith(identity).use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(1.second) >> err_fun(1), a1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServiceStop])

  }

  test("4.cancellation should propagate in right order") {
    val Vector(a, b, c, d) = task
      .service("order")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { ag =>
        val a1 = ag.action("one/two/inner", _.silent).retry(IO.never[Int]).buildWith(identity).use(_.run(()))
        ag.action("one/two/three/outer", _.silent)
          .retry(IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1)))
          .buildWith(identity)
          .use(_.run(()))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ActionFail].actionParams.metricName.digest == "7ef72bfa")
    assert(c.asInstanceOf[ActionFail].actionParams.metricName.digest == "17a1104e")
    assert(d.isInstanceOf[ServiceStop])
  }

  test("5.cancellation - sequentially - cancel after two complete") {
    val Vector(s, a, b, c, d, e) = task
      .service("sequentially")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { ag =>
        ag.action("a1", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(())) >>
          ag.action("a2", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(())) >>
          IO.canceled >>
          ag.action("a3", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionDone].actionParams.metricName.digest == "4a85d410")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionDone].actionParams.metricName.digest == "4e1bf824")
    assert(e.isInstanceOf[ServiceStop])

  }

  test("6.cancellation - sequentially - no chance to cancel") {
    val policy = policies.fixedDelay(1.seconds).limited(1)
    val Vector(s, a, b, c, d, e, f) = task
      .service("no cancel")
      .updateConfig(_.withRestartPolicy(policies.giveUp))
      .eventStream { ag =>
        ag.action("a1", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(())) >>
          ag.action("a2", _.bipartite.policy(policy))
            .retry(IO.raiseError[Int](new Exception))
            .buildWith(identity)
            .use(_.run(())) >>
          IO.canceled >> // no chance to cancel since a2 never success
          ag.action("a3", _.bipartite).retry(IO(1)).buildWith(identity).use(_.run(()))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionDone].actionParams.metricName.digest == "b40a0d4d")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionRetry].actionParams.metricName.digest == "970e74bc")
    assert(e.asInstanceOf[ActionFail].actionParams.metricName.digest == "970e74bc")
    assert(f.isInstanceOf[ServiceStop])
  }

  test("7.cancellation - parallel") {
    val policy: Policy = policies.fixedDelay(1.seconds).limited(3)
    val v: Vector[NJEvent] =
      task
        .service("parallel")
        .updateConfig(_.withRestartPolicy(policies.giveUp))
        .eventStream { ag =>
          val a1 =
            ag.action("complete-1", _.bipartite)
              .retry(IO.sleep(0.3.second).as(1))
              .buildWith(identity)
              .use(_.run(()))
          val a2 =
            ag.action("fail-2", _.bipartite.policy(policy))
              .retry(IO.raiseError[Int](new Exception))
              .buildWith(identity)
              .use(_.run(()))
          val a3 = ag.action("cancel-3", _.bipartite).retry(never_fun).buildWith(identity).use(_.run(()))
          ag.action("supervisor", _.bipartite)
            .retry(IO.parSequenceN(3)(List(a1, a2, a3)))
            .buildWith(identity)
            .use(_.run(()))
        }
        .map(checkJson)
        .compile
        .toVector
        .unsafeRunSync()

    assert(v(0).isInstanceOf[ServiceStart])
    assert(v(1).isInstanceOf[ActionStart])
    assert(v(2).isInstanceOf[ActionStart])
    assert(v(3).isInstanceOf[ActionStart])
    assert(v(4).isInstanceOf[ActionStart])

    assert(v(5).isInstanceOf[ActionRetry]) // a2
    assert(v(6).isInstanceOf[ActionDone]) // a1
    assert(v(7).isInstanceOf[ActionRetry]) // a2
    assert(v(8).isInstanceOf[ActionRetry]) // a2
    assert(v(9).isInstanceOf[ActionFail]) // a2 failed
    assert(v(10).isInstanceOf[ActionFail]) // a3 cancelled
    assert(v(11).isInstanceOf[ActionFail]) // supervisor
    assert(v(12).isInstanceOf[ServiceStop])
  }

  test("8.cancellation - cancel in middle of retrying") {
    val policy = policies.fixedDelay(2.seconds)
    val Vector(s, a, b, c, d, e) = task
      .service("cancel retry")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { ag =>
        val a1 =
          ag.action("exception", _.bipartite.policy(policy))
            .retry(IO.raiseError[Int](new Exception))
            .buildWith(identity)
            .use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(3.second) >> IO.canceled, a1))
      }
      .map(checkJson)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionFail])
    assert(e.isInstanceOf[ServiceStop])

  }

  test("9.cancellation - wrapped within cancelable") {
    val Vector(s, b, c, d, e, f) = task
      .service("wrap")
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
      .eventStream { ag =>
        val a1 = ag
          .action("exception", _.policy(policy))
          .retry(IO.raiseError[Int](new Exception))
          .buildWith(identity)
          .use(_.run(()))
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, IO.uncancelable(_ => a1)))
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

  test("10.cancellation - never can be canceled") {
    val Vector(a, b) =
      task
        .service("never")
        .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.hour)))
        .eventStream(_.action("never", _.bipartite).retry(IO.never[Int]).buildWith(identity).use(_.run(())))
        .map(checkJson)
        .interruptAfter(2.seconds)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
  }
}
