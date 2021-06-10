package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.kernel.Monoid
import cats.syntax.all._
import com.github.chenharryhua.nanjin.guard._
import com.github.chenharryhua.nanjin.guard.alert._
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

class RetryEitherTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("retry-either-guard-test")
    .updateConfig(_.withConstantDelay(1.second))
    .updateActionConfig(_.withConstantDelay(1.second).withFailAlertOn.withSuccAlertOn)
    .service("retry-either-test")
    .updateServiceConfig(_.withHealthCheckInterval(3.hours).withConstantDelay(1.seconds))

  val logging: AlertService[IO] = Monoid[AlertService[IO]].empty |+| ConsoleService[IO] |+| LogService[IO]

  test("retry either should give up immediately when outer action fails") {
    val Vector(a, b) = guard
      .updateServiceConfig(_.withStartUpDelay(2.hours).withConstantDelay(1.hour))
      .updateActionConfig(_.withSuccAlertOn.withExponentialBackoff(1.second))
      .eventStream(gd =>
        gd("retry-either-give-up")
          .retryEither(5)(_ => IO.raiseError(new Exception))
          .withSuccNotes((_, _: Unit) => null)
          .withFailNotes((_, e) => null)
          .run)
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionFailed])
    assert(b.isInstanceOf[ServicePanic])
  }

  test("retry either should retry 2 times to success") {
    var i = 0
    val Vector(a, b, c, d) = guard
      .updateServiceConfig(_.withStartUpDelay(2.hours).withConstantDelay(1.hour))
      .eventStream(gd =>
        gd("retry-either-2-times")
          .updateActionConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .retryEither("does not matter")(_ =>
            IO(if (i < 2) { i += 1; Left(new Exception("oops")) }
            else Right(1)))
          .run)
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionSucced])
    assert(d.isInstanceOf[ServiceStopped])
  }
  test("retry either should escalate if all attempts fail") {
    var i = 0
    val Vector(a, b, c, d, e) = guard
      .updateServiceConfig(_.withStartUpDelay(2.hours).withConstantDelay(1.hour))
      .eventStream(gd =>
        gd("retry-either-escalate")
          .updateActionConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .retryEither(IO(if (i < 200) { i += 1; Left(new Exception) }
          else Right(1)))
          .run)
      .observe(_.evalMap(logging.alert).drain)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionRetrying])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionFailed])
    assert(e.isInstanceOf[ServicePanic])
  }

}
