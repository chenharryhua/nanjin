package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class CancellationTest extends AnyFunSuite {
  val serviceGuard = TaskGuard[IO]("retry-guard").service("retry-test").updateConfig(_.withConstantDelay(1.seconds))

  test("cancellation - canceled actions are failed actions") {
    val Vector(s, a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(action =>
        action
          .span("canceled")
          .notice
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .retry(IO(1) >> IO.canceled)
          .run)
      .interruptAfter(7.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled internally")
    assert(f.isInstanceOf[ServicePanic])
  }

  test("cancellation - can be canceled externally") {
    val Vector(s, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action.span("never").normal.run(IO.never[Int])
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))
      }
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(b.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled externally")
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("compare to exception") {
    val Vector(s, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action.span("never").normal.run(IO.never[Int])
        IO.parSequenceN(2)(List(IO.sleep(1.second) >> IO.raiseError(new Exception), a1))
      }
      .interruptAfter(3.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(b.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled externally")
    assert(c.isInstanceOf[ServicePanic])
  }

  test("cancellation - can be protected from external cancel") {
    val Vector(s, c, d, f, g, h) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { ag =>
        val action = ag.updateConfig(_.withConstantDelay(1.second).withMaxRetries(1))
        val a1     = action.span("never").run(IO.never[Int])
        action.span("supervisor").retry(IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))).run
      }
      .interruptAfter(10.second)
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(c.asInstanceOf[ActionFailed].actionInfo.actionParams.metricName.value == "never/86d0fbe4")
    assert(c.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled externally")
    assert(d.asInstanceOf[ActionRetrying].actionInfo.actionParams.metricName.value == "supervisor/f154e9cf")
    assert(f.asInstanceOf[ActionFailed].actionInfo.actionParams.metricName.value == "never/86d0fbe4")
    assert(f.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled externally")
    assert(g.asInstanceOf[ActionFailed].actionInfo.actionParams.metricName.value == "supervisor/f154e9cf")
    assert(g.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled internally")
    assert(h.isInstanceOf[ServicePanic])
  }

  test("cancellation - sequentially - cancel after two succ") {
    val Vector(s, a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        action.span("a1").notice.retry(IO(1)).run >>
          action.span("a2").notice.retry(IO(1)).run >>
          IO.canceled >>
          action.span("a3").notice.retry(IO(1)).run
      }
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionSucced].actionInfo.actionParams.metricName.value == "a1/6f340f3f")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionSucced].actionInfo.actionParams.metricName.value == "a2/56199b40")
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("cancellation - sequentially - no chance to cancel") {
    val Vector(s, a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        action.span("a1").notice.retry(IO(1)).run >>
          action
            .span("a2")
            .notice
            .updateConfig(_.withConstantDelay(1.second).withMaxRetries(1))
            .retry(IO.raiseError(new Exception))
            .run >>
          IO.canceled >> // no chance to cancel since a2 never success
          action.span("a3").notice.retry(IO(1)).run
      }
      .interruptAfter(5.second)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionSucced].actionInfo.actionParams.metricName.value == "a1/6f340f3f")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionRetrying].actionInfo.actionParams.metricName.value == "a2/56199b40")
    assert(e.asInstanceOf[ActionFailed].actionInfo.actionParams.metricName.value == "a2/56199b40")
    assert(e.asInstanceOf[ActionFailed].error.message == "Exception: ")
    assert(f.isInstanceOf[ServicePanic])
  }

  test("cancellation - parallel") {
    val v =
      serviceGuard
        .updateConfig(_.withConstantDelay(1.hour))
        .eventStream { action =>
          val a1 = action.span("succ-1").notice.run(IO.sleep(1.second) >> IO(1))
          val a2 = action
            .span("fail-2")
            .notice
            .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
            .run(IO.raiseError[Int](new Exception))
          val a3 = action.span("cancel-3").notice.run(IO.never[Int])
          action
            .span("supervisor")
            .notice
            .updateConfig(_.withMaxRetries(1).withConstantDelay(1.second))
            .run(IO.parSequenceN(5)(List(a1, a2, a3)))
        }
        .interruptAfter(10.second)
        .compile
        .toVector
        .unsafeRunSync()

    assert(v(0).isInstanceOf[ServiceStarted])
    assert(v(1).isInstanceOf[ActionStart])
    assert(v(2).isInstanceOf[ActionStart])
    assert(v(3).isInstanceOf[ActionStart])
    assert(v(4).isInstanceOf[ActionStart])

    assert(v(5).isInstanceOf[ActionRetrying]) // a2
    assert(v(6).isInstanceOf[ActionSucced] || v(6).isInstanceOf[ActionRetrying]) // a1
    assert(v(7).isInstanceOf[ActionRetrying] || v(7).isInstanceOf[ActionSucced]) // a2
    assert(v(8).isInstanceOf[ActionRetrying]) // a2
    assert(v(9).isInstanceOf[ActionFailed]) // a2 failed
    assert(v(10).isInstanceOf[ActionFailed]) // a3 cancelled

    assert(v(11).isInstanceOf[ActionRetrying]) // supervisor
    assert(v(12).isInstanceOf[ActionStart])
    assert(v(13).isInstanceOf[ActionStart])
    assert(v(14).isInstanceOf[ActionStart])

    assert(v(15).isInstanceOf[ActionRetrying] || v(15).isInstanceOf[ActionSucced]) // a1 or a2
    assert(v(16).isInstanceOf[ActionSucced] || v(16).isInstanceOf[ActionRetrying]) // a1 or a2
    assert(v(17).isInstanceOf[ActionRetrying] || v(17).isInstanceOf[ActionSucced]) // a1 or a2
    assert(v(18).isInstanceOf[ActionRetrying]) // a2
    assert(v(19).isInstanceOf[ActionFailed]) // a2 failed
    assert(v(20).isInstanceOf[ActionFailed]) // a3 cancelled
    assert(v(21).isInstanceOf[ActionFailed]) // supervisor
    assert(v(22).isInstanceOf[ServicePanic])
  }

  test("cancellation - cancel in middle of retrying") {
    val Vector(s, a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action
          .span("exception")
          .notice
          .updateConfig(_.withConstantDelay(2.second).withMaxRetries(100))
          .retry(IO.raiseError[Int](new Exception))
          .run
        IO.parSequenceN(2)(List(IO.sleep(3.second) >> IO.canceled, a1))
      }
      .compile
      .toVector
      .unsafeRunSync()
    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled externally")
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("cancellation - wrapped within uncancelable") {
    val Vector(s, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action
          .span("exception")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .retry(IO.raiseError[Int](new Exception))
          .run
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, IO.uncancelable(_ => a1)))
      }
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServicePanic])
  }
}
