package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.alert.{ActionFailed, ActionSucced, AlertService, NJEvent}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

final class AlertStub(var succ: Int, var fail: Int) extends AlertService[IO] {

  override def alert(event: NJEvent): IO[Unit] = event match {
    case _: ActionSucced => IO(succ += 1)
    case _: ActionFailed => IO(fail += 1)
    case _               => IO.unit
  }
}

class GroupTest extends AnyFunSuite {
  val taskGuard: TaskGuard[IO] = TaskGuard[IO]("group-test").updateGroupConfig(_.withConstantDelay(1.second))
  test("should run to end without error") {
    val gd = taskGuard.group("happy-path").updateGroupConfig(_.withFullJitter(1.second).withMaxRetries(3))
    val Vector(a, b) = gd
      .eventStream(ag => ag("g1").retry(IO(1)).run >> ag("g2").retryEither(IO(Right("a"))).run)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ActionSucced])
  }

  test("should retry a few times if error occurs") {
    val stub = new AlertStub(0, 0)
    val gd = taskGuard
      .group("always-fail")
      .updateGroupConfig(_.withFibonacciBackoff(1.second).withMaxRetries(3))
      .updateActionConfig(_.withMaxRetries(0).withConstantDelay(1.second))
    val run = gd
      .eventStream(ag => ag("g1").retry(IO(1)).run >> ag("g2").retryEither(IO(Left(new Exception))).run)
      .evalMap(stub.alert)
      .compile
      .drain
    assertThrows[Exception](run.unsafeRunSync())
    assert(stub.succ == stub.fail)
  }

  test("should success if retry works") {
    val gd = taskGuard
      .group("finally-succ")
      .updateGroupConfig(_.withExponentialBackoff(1.second).withMaxRetries(3))
      .updateActionConfig(_.withMaxRetries(0).withConstantDelay(1.second))
    var i = 0
    val Vector(a, b, c, d, e, f) = gd
      .eventStream(ag =>
        ag("g1").retry(IO(1)).run >> ag("g2")
          .retryEither(IO(if (i < 2) { i += 1; Left(new Exception) }
          else Right("b")))
          .run)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ActionFailed])
    assert(c.isInstanceOf[ActionSucced])
    assert(d.isInstanceOf[ActionFailed])
    assert(e.isInstanceOf[ActionSucced])
    assert(f.isInstanceOf[ActionSucced])
  }
}
