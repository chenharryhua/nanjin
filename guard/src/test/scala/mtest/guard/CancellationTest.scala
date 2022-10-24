package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import java.time.ZoneId
import scala.concurrent.duration.*

class CancellationTest extends AnyFunSuite {

  val serviceGuard: ServiceGuard[IO] =
    TaskGuard[IO]("retry-guard").service("retry-test").withRestartPolicy(constant_1second)

  val policy = policies.cronBackoff[IO](secondly, ZoneId.systemDefault()).join(RetryPolicies.limitRetries(3))

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
  }

  test("3.canceled by external exception") {
    val Vector(s, b, c) = serviceGuard
      .withRestartPolicy(RetryPolicies.alwaysGiveUp)
      .eventStream { ag =>
        val a1 = ag.action("never", _.trivial).retry(never_fun).run
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
      .withRestartPolicy(RetryPolicies.alwaysGiveUp)
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
    assert(b.asInstanceOf[ActionFail].actionParams.digested.metricRepr == "[one/two/inner][89f90a0c]")
    assert(c.asInstanceOf[ActionFail].actionParams.digested.metricRepr == "[one/two/three/outer][59553dec]")
    assert(d.isInstanceOf[ServiceStop])
  }

  test("5.cancellation - sequentially - cancel after two succ") {
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
    assert(b.asInstanceOf[ActionSucc].actionParams.digested.metricRepr == "[a1][6f340f3f]")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionSucc].actionParams.digested.metricRepr == "[a2][56199b40]")
    assert(e.isInstanceOf[ServiceStop])

  }

  test("6.cancellation - sequentially - no chance to cancel") {
    val policy = RetryPolicies.constantDelay[IO](1.seconds).join(RetryPolicies.limitRetries(1))
    val Vector(s, a, b, c, d, e, f) = serviceGuard
      .withRestartPolicy(RetryPolicies.alwaysGiveUp)
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
    assert(b.asInstanceOf[ActionSucc].actionParams.digested.metricRepr == "[a1][6f340f3f]")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionRetry].actionParams.digested.metricRepr == "[a2][56199b40]")
    assert(e.asInstanceOf[ActionFail].actionParams.digested.metricRepr == "[a2][56199b40]")
    assert(f.isInstanceOf[ServiceStop])

  }

  test("7.cancellation - parallel") {
    val policy2 = RetryPolicies.constantDelay[IO](1.seconds).join(RetryPolicies.limitRetries(1))
    val v =
      serviceGuard
        .withRestartPolicy(RetryPolicies.alwaysGiveUp)
        .eventStream { ag =>
          val a1 = ag.action("succ-1", _.notice).retry(IO.sleep(1.second) >> IO(1)).run
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
    assert(v(6).isInstanceOf[ActionSucc] || v(6).isInstanceOf[ActionRetry]) // a1
    assert(v(7).isInstanceOf[ActionRetry] || v(7).isInstanceOf[ActionSucc]) // a2
    assert(v(8).isInstanceOf[ActionRetry]) // a2
    assert(v(9).isInstanceOf[ActionFail]) // a2 failed
    assert(v(10).isInstanceOf[ActionFail]) // a3 cancelled

    assert(v(11).isInstanceOf[ActionRetry]) // supervisor
    assert(v(12).isInstanceOf[ActionStart])
    assert(v(13).isInstanceOf[ActionStart])
    assert(v(14).isInstanceOf[ActionStart])

    assert(v(15).isInstanceOf[ActionRetry] || v(15).isInstanceOf[ActionSucc]) // a1 or a2
    assert(v(16).isInstanceOf[ActionSucc] || v(16).isInstanceOf[ActionRetry]) // a1 or a2
    assert(v(17).isInstanceOf[ActionRetry] || v(17).isInstanceOf[ActionSucc]) // a1 or a2
    assert(v(18).isInstanceOf[ActionRetry]) // a2
    assert(v(19).isInstanceOf[ActionFail]) // a2 failed
    assert(v(20).isInstanceOf[ActionFail]) // a3 cancelled
    assert(v(21).isInstanceOf[ActionFail]) // supervisor
    assert(v(22).isInstanceOf[ServiceStop])
  }

  test("8.cancellation - cancel in middle of retrying") {
    val policy = RetryPolicies.constantDelay[IO](2.seconds)
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
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
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
