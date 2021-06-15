package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard._
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionRetrying,
  ActionSucced,
  ServicePanic,
  ServiceStopped
}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class CancellationTest extends AnyFunSuite {
  val serviceGuard = TaskGuard[IO]("retry-guard")
    .service("retry-test")
    .updateConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  test("cancellation - canceled actions are failed actions") {
    val Vector(a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream(action =>
        action("canceled").updateConfig(_.withConstantDelay(1.second).withMaxRetries(3)).run(IO(1) >> IO.canceled))
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled")
    assert(e.isInstanceOf[ServicePanic])
  }

  test("cancellation - can be cancelled externally") {
    val Vector(a, b) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartUpDelay(1.hour))
      .eventStream { action =>
        val a1 = action("never").run(IO.never[Int])
        IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1))
      }
      .compile
      .toVector
      .unsafeRunSync()

    assert(
      a.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled by asynchronous exception")
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("compare to exception") {
    val Vector(a, b) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartUpDelay(1.hour))
      .eventStream { action =>
        val a1 = action("never").run(IO.never[Int])
        IO.parSequenceN(2)(List(IO.sleep(1.second) >> IO.raiseError(new Exception), a1))
      }
      .interruptAfter(3.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(
      a.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled by asynchronous exception")
    assert(b.isInstanceOf[ServicePanic])
  }

  test("cancellation - can be protected from external cancel") {
    val Vector(a, b, c, d, e) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartUpDelay(1.hour))
      .eventStream { ag =>
        val action = ag.updateConfig(_.withConstantDelay(1.second).withMaxRetries(1))
        val a1     = action("never").run(IO.never[Int])
        action("supervisor").run(IO.parSequenceN(2)(List(IO.sleep(2.second) >> IO.canceled, a1)))
      }
      .interruptAfter(10.second)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.asInstanceOf[ActionFailed].actionInfo.actionName == "never")
    assert(
      a.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled by asynchronous exception")
    assert(b.asInstanceOf[ActionRetrying].actionInfo.actionName == "supervisor")
    assert(c.asInstanceOf[ActionFailed].actionInfo.actionName == "never")
    assert(
      c.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled by asynchronous exception")
    assert(d.asInstanceOf[ActionFailed].actionInfo.actionName == "supervisor")
    assert(d.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled")
    assert(e.isInstanceOf[ServicePanic])
  }

  test("cancellation - sequentially - cancel after two succ") {
    val Vector(a, b, c) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartUpDelay(1.hour))
      .eventStream { action =>
        action("a1").run(IO(1)) >>
          action("a2").run(IO(1)) >>
          IO.canceled >>
          action("a3").run(IO(1))
      }
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.asInstanceOf[ActionSucced].actionInfo.actionName == "a1")
    assert(b.asInstanceOf[ActionSucced].actionInfo.actionName == "a2")
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("cancellation - sequentially - no chance to cancel") {
    val Vector(a, b, c, d) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartUpDelay(1.hour))
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

    assert(a.asInstanceOf[ActionSucced].actionInfo.actionName == "a1")
    assert(b.asInstanceOf[ActionRetrying].actionInfo.actionName == "a2")
    assert(c.asInstanceOf[ActionFailed].actionInfo.actionName == "a2")
    assert(c.asInstanceOf[ActionFailed].error.message == "Exception: ")
    assert(d.isInstanceOf[ServicePanic])
  }

  test("cancellation - parallel") {
    val Vector(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12, a13, a14, a15) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour))
      .eventStream { action =>
        val a1 = action("succ-1").run(IO.sleep(1.second) >> IO(1))
        val a2 = action("fail-2").updateConfig(_.withConstantDelay(1.second)).run(IO.raiseError[Int](new Exception))
        val a3 = action("cancel-3").run(IO.never[Int])
        action("supervisor")
          .updateConfig(_.withMaxRetries(1).withConstantDelay(1.second))
          .run(IO.parSequenceN(5)(List(a1, a2, a3)))
      }
      .debug()
      .interruptAfter(10.second)
      .compile
      .toVector
      .unsafeRunSync()

    assert(a1.isInstanceOf[ActionRetrying]) // a2
    assert(a2.isInstanceOf[ActionSucced] || a2.isInstanceOf[ActionRetrying]) // a1
    assert(a3.isInstanceOf[ActionRetrying] || a3.isInstanceOf[ActionSucced]) // a2
    assert(a4.isInstanceOf[ActionRetrying]) // a2
    assert(a5.isInstanceOf[ActionFailed]) // a2 failed
    assert(a6.isInstanceOf[ActionFailed]) // a3 cancelled
    //
    assert(a7.isInstanceOf[ActionRetrying]) // supervisor
    assert(a8.isInstanceOf[ActionRetrying] || a8.isInstanceOf[ActionSucced]) // a1 or a2
    assert(a9.isInstanceOf[ActionSucced] || a9.isInstanceOf[ActionRetrying]) // a1 or a2
    assert(a10.isInstanceOf[ActionRetrying] || a10.isInstanceOf[ActionSucced]) // a1 or a2
    assert(a11.isInstanceOf[ActionRetrying]) // a2
    assert(a12.isInstanceOf[ActionFailed]) // a2 failed
    assert(a13.isInstanceOf[ActionFailed]) // a3 cancelled
    assert(a14.isInstanceOf[ActionFailed]) // supervisor
    assert(a15.isInstanceOf[ServicePanic])
  }

  test("cancellation - cancel in middle of retrying") {
    val Vector(a, b, c, d) = serviceGuard
      .updateConfig(_.withConstantDelay(1.hour).withStartUpDelay(1.hour))
      .eventStream { action =>
        val a1 = action("exception")
          .updateConfig(_.withConstantDelay(2.second).withMaxRetries(100))
          .run(IO.raiseError[Int](new Exception))
        IO.parSequenceN(2)(List(IO.sleep(3.second) >> IO.canceled, a1))
      }
      .compile
      .toVector
      .unsafeRunSync()

    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(
      c.asInstanceOf[ActionFailed].error.message == "Exception: the action was cancelled by asynchronous exception")
    assert(d.isInstanceOf[ServiceStopped])
  }
}
