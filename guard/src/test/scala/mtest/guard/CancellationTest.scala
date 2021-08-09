package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.action.ActionException
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionRetrying,
  ActionStart,
  ActionSucced,
  ServicePanic,
  ServiceStopped
}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

class CancellationTest extends AnyFunSuite {
  val serviceGuard = TaskGuard[IO]("retry-guard")
    .service("retry-test")
    .addAlertService(slack)
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  test("cancellation - canceled actions are failed actions") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(action =>
        action("canceled").updateConfig(_.withConstantDelay(1.second).withMaxRetries(3)).run(IO(1) >> IO.canceled))
      .interruptAfter(7.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledInternally])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("cancellation - can be canceled externally") {
    val Vector(a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { action =>
        val a1 = action("never").run(IO.never[Int])
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))
      }
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledExternally])
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("compare to exception") {
    val Vector(a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { action =>
        val a1 = action("never").run(IO.never[Int])
        IO.parSequenceN(2)(List(IO.sleep(1.second) >> IO.raiseError(new Exception), a1))
      }
      .interruptAfter(3.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledExternally])
    assert(c.isInstanceOf[ServicePanic])
  }

  test("cancellation - can be protected from external cancel") {
    val Vector(a, b, c, d, e, f, g, h) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { ag =>
        val action = ag.updateConfig(_.withConstantDelay(1.second).withMaxRetries(1))
        val a1     = action("never").run(IO.never[Int])
        action("supervisor").run(IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1)))
      }
      .debug()
      .interruptAfter(10.second)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.asInstanceOf[ActionFailed].actionInfo.actionName == "never")
    assert(c.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledExternally])
    assert(d.asInstanceOf[ActionRetrying].actionInfo.actionName == "supervisor")
    assert(e.isInstanceOf[ActionStart])
    assert(f.asInstanceOf[ActionFailed].actionInfo.actionName == "never")
    assert(f.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledExternally])
    assert(g.asInstanceOf[ActionFailed].actionInfo.actionName == "supervisor")
    assert(g.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledInternally])
    assert(h.isInstanceOf[ServicePanic])
  }

  test("cancellation - sequentially - cancel after two succ") {
    val Vector(a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { action =>
        action("a1").run(IO(1)) >>
          action("a2").run(IO(1)) >>
          IO.canceled >>
          action("a3").run(IO(1))
      }
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionSucced].actionInfo.actionName == "a1")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionSucced].actionInfo.actionName == "a2")
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("cancellation - sequentially - no chance to cancel") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { action =>
        action("a1").run(IO(1)) >>
          action("a2")
            .updateConfig(_.withConstantDelay(1.second).withMaxRetries(1))
            .run(IO.raiseError(new Exception)) >>
          IO.canceled >> // no chance to cancel since a2 never success
          action("a3").run(IO(1))
      }
      .interruptAfter(5.second)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionSucced].actionInfo.actionName == "a1")
    assert(c.isInstanceOf[ActionStart])
    assert(d.asInstanceOf[ActionRetrying].actionInfo.actionName == "a2")
    assert(e.asInstanceOf[ActionFailed].actionInfo.actionName == "a2")
    assert(e.asInstanceOf[ActionFailed].error.message == "Exception: ")
    assert(f.isInstanceOf[ServicePanic])
  }

  test("cancellation - parallel") {
    val v =
      serviceGuard
        .updateConfig(_.withConstantDelay(1.hour))
        .eventStream { action =>
          val a1 = action("succ-1").run(IO.sleep(1.second) >> IO(1))
          val a2 = action("fail-2")
            .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
            .run(IO.raiseError[Int](new Exception))
          val a3 = action("cancel-3").run(IO.never[Int])
          action("supervisor")
            .updateConfig(_.withMaxRetries(1).withConstantDelay(1.second))
            .run(IO.parSequenceN(5)(List(a1, a2, a3)))
        }
        .interruptAfter(10.second)
        .compile
        .toVector
        .unsafeRunSync()
    assert(v(0).isInstanceOf[ActionStart])
    assert(v(1).isInstanceOf[ActionStart])
    assert(v(2).isInstanceOf[ActionStart])
    assert(v(3).isInstanceOf[ActionStart])

    assert(v(4).isInstanceOf[ActionRetrying]) // a2
    assert(v(5).isInstanceOf[ActionSucced] || v(5).isInstanceOf[ActionRetrying]) // a1
    assert(v(6).isInstanceOf[ActionRetrying] || v(6).isInstanceOf[ActionSucced]) // a2
    assert(v(7).isInstanceOf[ActionRetrying]) // a2
    assert(v(8).isInstanceOf[ActionFailed]) // a2 failed
    assert(v(9).isInstanceOf[ActionFailed]) // a3 cancelled

    assert(v(10).isInstanceOf[ActionRetrying]) // supervisor
    assert(v(11).isInstanceOf[ActionStart])
    assert(v(12).isInstanceOf[ActionStart])
    assert(v(13).isInstanceOf[ActionStart])

    assert(v(14).isInstanceOf[ActionRetrying] || v(14).isInstanceOf[ActionSucced]) // a1 or a2
    assert(v(15).isInstanceOf[ActionSucced] || v(15).isInstanceOf[ActionRetrying]) // a1 or a2
    assert(v(16).isInstanceOf[ActionRetrying] || v(16).isInstanceOf[ActionSucced]) // a1 or a2
    assert(v(17).isInstanceOf[ActionRetrying]) // a2
    assert(v(18).isInstanceOf[ActionFailed]) // a2 failed
    assert(v(19).isInstanceOf[ActionFailed]) // a3 cancelled
    assert(v(20).isInstanceOf[ActionFailed]) // supervisor
    assert(v(21).isInstanceOf[ServicePanic])
  }

  test("cancellation - cancel in middle of retrying") {
    val Vector(a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { action =>
        val a1 = action("exception")
          .updateConfig(_.withConstantDelay(2.second).withMaxRetries(100))
          .run(IO.raiseError[Int](new Exception))
        IO.parSequenceN(2)(List(IO.sleep(3.second) >> IO.canceled, a1))
      }
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionFailed].error.throwable.isInstanceOf[ActionException.ActionCanceledExternally])
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("cancellation - wrapped within uncancelable") {
    val Vector(a, b, c, d, e, f) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartupDelay(1.hour))
      .eventStream { action =>
        val a1 = action("exception")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .run(IO.raiseError[Int](new Exception))
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, IO.uncancelable(_ => a1)))
      }
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServiceStopped])
  }
}
