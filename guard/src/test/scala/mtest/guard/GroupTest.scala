package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.alert.{ActionSucced, AlertService, NJEvent}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

final class AlertStub(var succ: Int) extends AlertService[IO] {

  override def alert(event: NJEvent): IO[Unit] = event match {
    case _: ActionSucced => IO(succ += 1)
    case _               => IO.unit
  }
}

class GroupTest extends AnyFunSuite {
  val taskGuard: TaskGuard[IO] = TaskGuard[IO]("group-test")
  test("should run to end without error") {
    val gd = taskGuard.group("happy-path").updateGroupConfig(_.withFullJitter(1.second).withMaxRetries(3))
    val Vector(a, b) = gd
      .eventStream(ag => ag("g1").retry(IO(1)).run >> ag("g2").retryEither(IO(Right("a"))).run)
      .debug()
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ActionSucced])
    assert(b.isInstanceOf[ActionSucced])
  }

  test("should retry a few times if error occurs") {
    val stub = new AlertStub(0)
    val gd = taskGuard
      .group("always-fail")
      .updateGroupConfig(_.withFibonacciBackoff(1.second).withMaxRetries(3).withTopicMaxQueued(10))
      .updateActionConfig(_.withMaxRetries(1))
    val run = gd
      .eventStream(ag => ag("g1").retry(IO(1)).run >> IO.raiseError(new Exception("oops")))
      .debug()
      .evalMap(stub.alert)
      .compile
      .drain
    assertThrows[Exception](run.unsafeRunSync())
    assert(stub.succ == 3)
  }

}
