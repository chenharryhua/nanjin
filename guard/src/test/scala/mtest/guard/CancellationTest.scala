package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class CancellationTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard").service("retry-test").withRestartPolicy(constant_1second)

  val policy = policies.crontab(cron_1second).limited(3)

  test("1.cancellation - canceled actions are failed actions") {
    val Vector(a, b, c, d) = serviceGuard
      .withRestartPolicy(constant_1hour)
      .eventStream(ag =>
        ag.action("canceled", _.notice).withRetryPolicy(policy).retry(IO(1) <* IO.canceled).run)
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ServiceStop])
    assert(d.asInstanceOf[ServiceStop].cause.isInstanceOf[ServiceStopCause.ByCancelation.type])

  }

  test("2.cancellation - can be canceled externally") {
    val Vector(s, b, c) = serviceGuard
      .withRestartPolicy(constant_1hour)
      .eventStream { ag =>
        val a1 = ag.action("never", _.silent).retry(never_fun).run
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServiceStop])
    assert(c.asInstanceOf[ServiceStop].cause.exitCode == 1)
  }

  test("3.canceled by external exception") {
    val Vector(s, b, c) = serviceGuard
      .withRestartPolicy(policies.giveUp)
      .eventStream { ag =>
        val a1 = ag.action("never").retry(never_fun).run
        IO.parSequenceN(2)(List(IO.sleep(1.second) >> err_fun(1), a1))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServiceStop])

  }

  test("4.cancellation should propagate in right order") {
    val Vector(a, b, c, d) = serviceGuard
      .withRestartPolicy(policies.giveUp)
      .eventStream { ag =>
        val a1 = ag.action("one/two/inner", _.silent).retry(IO.never[Int]).run
        ag.action("one/two/three/outer", _.silent)
          .retry(IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1)))
          .run
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ActionFail].actionParams.metricId.metricName.digest == "08e76668")
    assert(c.asInstanceOf[ActionFail].actionParams.metricId.metricName.digest == "3d5f88dc")
    assert(d.isInstanceOf[ServiceStop])
  }

  test("5.cancellation - sequentially - cancel after two complete") {
    val Vector(s, a, b, c, d, e) = serviceGuard
      .withRestartPolicy(constant_1hour)
      .eventStream { ag =>
        ag.action("a1", _.notice).retry(IO(1)).run >>
          ag.action("a2", _.notice).retry(IO(1)).run >>
          IO.canceled >>
          ag.action("a3", _.notice).retry(IO(1)).run
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionDone].actionParams.metricId.metricName.digest == "104c9af4")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionDone].actionParams.metricId.metricName.digest == "fec6047b")
    assert(e.isInstanceOf[ServiceStop])

  }

  test("6.cancellation - sequentially - no chance to cancel") {
    val policy = policies.fixedDelay(1.seconds).limited(1)
    val Vector(s, a, b, c, d, e, f) = serviceGuard
      .withRestartPolicy(policies.giveUp)
      .eventStream { ag =>
        ag.action("a1", _.notice).retry(IO(1)).run >>
          ag.action("a2", _.notice).withRetryPolicy(policy).retry(IO.raiseError(new Exception)).run >>
          IO.canceled >> // no chance to cancel since a2 never success
          ag.action("a3", _.notice).retry(IO(1)).run
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionDone].actionParams.metricId.metricName.digest == "104c9af4")
    assert(!b.asInstanceOf[ActionDone].took.isNegative)
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionRetry].actionParams.metricId.metricName.digest == "fec6047b")
    assert(e.asInstanceOf[ActionFail].actionParams.metricId.metricName.digest == "fec6047b")
    assert(f.isInstanceOf[ServiceStop])
  }

  test("7.cancellation - parallel") {
    val policy2 = constant_1second.limited(1)
    val v =
      serviceGuard
        .withRestartPolicy(policies.giveUp)
        .eventStream { ag =>
          val a1 = ag.action("complete-1", _.notice).retry(IO.sleep(1.second) >> IO(1)).run
          val a2 =
            ag.action("fail-2", _.notice).withRetryPolicy(policy).retry(IO.raiseError[Int](new Exception)).run
          val a3 = ag.action("cancel-3", _.notice).retry(never_fun).run
          ag.action("supervisor", _.notice)
            .withRetryPolicy(policy2)
            .retry(IO.parSequenceN(5)(List(a1, a2, a3)))
            .run
        }
        .map(_.asJson.noSpaces)
        .evalMap(e => IO(decode[NJEvent](e)).rethrow)
        .compile
        .toVector
        .unsafeRunSync()

    assert(v(0).isInstanceOf[ServiceStart])
    assert(v(1).isInstanceOf[ActionStart])
    assert(v(2).isInstanceOf[ActionStart])
    assert(v(3).isInstanceOf[ActionStart])
    assert(v(4).isInstanceOf[ActionStart])

    assert(v(5).isInstanceOf[ActionRetry]) // a2
    assert(v(6).isInstanceOf[ActionDone] || v(6).isInstanceOf[ActionRetry]) // a1
    assert(v(7).isInstanceOf[ActionRetry] || v(7).isInstanceOf[ActionDone]) // a2
    assert(v(8).isInstanceOf[ActionRetry]) // a2
    assert(v(9).isInstanceOf[ActionFail]) // a2 failed
    assert(v(10).isInstanceOf[ActionFail]) // a3 cancelled

    assert(v(11).isInstanceOf[ActionRetry]) // supervisor
    assert(v(12).isInstanceOf[ActionStart])
    assert(v(13).isInstanceOf[ActionStart])
    assert(v(14).isInstanceOf[ActionStart])

    assert(v(15).isInstanceOf[ActionRetry] || v(15).isInstanceOf[ActionDone]) // a1 or a2
    assert(v(16).isInstanceOf[ActionDone] || v(16).isInstanceOf[ActionRetry]) // a1 or a2
    assert(v(17).isInstanceOf[ActionRetry] || v(17).isInstanceOf[ActionDone]) // a1 or a2
    assert(v(18).isInstanceOf[ActionRetry]) // a2
    assert(v(19).isInstanceOf[ActionFail]) // a2 failed
    assert(v(20).isInstanceOf[ActionFail]) // a3 cancelled
    assert(v(21).isInstanceOf[ActionFail]) // supervisor
    assert(v(22).isInstanceOf[ServiceStop])
  }

  test("8.cancellation - cancel in middle of retrying") {
    val policy = policies.fixedDelay(2.seconds)
    val Vector(s, a, b, c, d, e) = serviceGuard
      .withRestartPolicy(constant_1hour)
      .eventStream { ag =>
        val a1 = ag
          .action("exception", _.notice)
          .withRetryPolicy(policy)
          .retry(IO.raiseError[Int](new Exception))
          .run
        IO.parSequenceN(2)(List(IO.sleep(3.second) >> IO.canceled, a1))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionFail])
    assert(!d.asInstanceOf[ActionFail].took.isNegative)
    assert(e.isInstanceOf[ServiceStop])
  }

  test("9.cancellation - wrapped within uncancelable") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .withRestartPolicy(constant_1hour)
      .eventStream { ag =>
        val a1 = ag.action("exception").withRetryPolicy(policy).retry(IO.raiseError[Int](new Exception)).run
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, IO.uncancelable(_ => a1)))
      }
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
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
    var i = 0
    serviceGuard
      .withRestartPolicy(constant_1hour)
      .eventStream(_.action("never").retry(IO.never.onCancel(IO { i = 1 })).run)
      .map(_.asJson.noSpaces)
      .evalMap(e => IO(decode[NJEvent](e)).rethrow)
      .interruptAfter(2.seconds)
      .compile
      .drain
      .unsafeRunSync()
    assert(i == 1)
  }

}
