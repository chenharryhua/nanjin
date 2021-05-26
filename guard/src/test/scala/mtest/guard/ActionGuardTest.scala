package mtest.guard

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Sync}
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.{
  ActionFailed,
  ActionRetrying,
  ActionSucced,
  AlertService,
  SlackService,
  Status,
  TaskGuard
}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

final class ExceptionService extends AlertService[IO] {
  override def alert(status: Status): IO[Unit] = IO.raiseError(new Exception("oops"))
}

final class CountService(var countRetries: Int, var numRetries: Int, var failed: Int) extends AlertService[IO] {

  override def alert(status: Status): IO[Unit] = status match {
    case _: ActionRetrying              => IO(countRetries += 1)
    case ActionSucced(_, _, _, retries) => IO(numRetries += retries)
    case _: ActionFailed                => IO(failed += 1)
    case _                              => IO.unit
  }
}

class ActionGuardTest extends AnyFunSuite {

  val guard: TaskGuard[IO] =
    TaskGuard[IO]("ActionTest")
      .addAlertService(SlackService(SimpleNotificationService.fake[IO]))
      .addAlertService(new ExceptionService)

  test("should not crash when evaluate") {
    val other = new CountService(0, 0, 0)
    val action = guard
      .addAlertService(other)
      .action("crash test")
      .updateConfig(_.withMaxRetries(3).withExponentialBackoff(1.second).failOn.succOn)
    val res =
      (guard.fyi("start") >> action.retry(IO(1)).withSuccInfo((_, _) => "ok").withFailInfo((_, _) => "oops").run)
        .unsafeRunSync()
    assert(other.numRetries == other.countRetries)
    assert(other.failed == 0)
    assert(res == 1)
  }
  test("should able to retry many times when operation fail") {
    val other = new CountService(0, 0, 0)
    val action =
      guard.addAlertService(other).action("retry test").updateConfig(_.withMaxRetries(3).withConstantDelay(1.second))

    var i = 0
    val op: IO[Int] = IO(
      if (i < 3) { i += 1; throw new Exception }
      else 1)
    val res = action.retry(op).run.unsafeRunSync()
    assert(other.numRetries == other.countRetries)
    assert(other.failed == 0)
    assert(res == 1)
  }
  test("should fail if can not success in MaxRetries") {
    val other = new CountService(0, 0, 0)
    val action = guard
      .addAlertService(other)
      .action("fail retry")
      .updateConfig(_.withMaxRetries(3).withFullJitter(1.second).failOn)

    var i = 0
    val op: IO[Int] = IO(
      if (i < 4) { i += 1; throw new Exception }
      else 1)
    assertThrows[Exception](action.retry(op).run.unsafeRunSync())
    assert(other.countRetries == 3)
    assert(other.numRetries == 0)
    assert(other.failed == 1)
  }
  test("eval fib") {
    val action = guard.action("fib").updateConfig(_.withMaxRetries(3).withFibonacciBackoff(1.second).failOn)
    val res    = action.retry(IO(1)).withSuccInfo((_, _) => "succ").withFailInfo((_, _) => "fail").run.unsafeRunSync()
    assert(res == 1)
  }
}
