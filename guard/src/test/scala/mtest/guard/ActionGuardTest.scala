package mtest.guard

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Sync}
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.{AlertService, SlackService, Status, TaskGuard}
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._

final class ExceptionService extends AlertService[IO] {
  override def alert(status: Status)(implicit F: Sync[IO]): IO[Unit] = IO(throw new Exception("oops"))
}

final class CountService(var count: Int) extends AlertService[IO] {
  override def alert(status: Status)(implicit F: Sync[IO]): IO[Unit] = IO(count += 1)
}

class ActionGuardTest extends AnyFunSuite {

  val guard: TaskGuard[IO] =
    TaskGuard[IO]
      .addAlertService(SlackService(SimpleNotificationService.fake[IO]))
      .addAlertService(new ExceptionService)
      .withApplicationName("ActionTest")

  test("should not crash when evaluate") {
    val other = new CountService(0)
    val action = guard
      .addAlertService(other)
      .action
      .updateConfig(
        _.withMaxRetries(3)
          .withConstantDelay(1.second)
          .withExponentialBackoff(1.second)
          .withFullJitter(1.second)
          .withFibonacciBackoff(1.second)
          .withFailOn
          .withSuccOn)
    val res = action.retry(IO(1)).withSucc((_, _) => "ok").withFail((_, _) => "oops").run.unsafeRunSync()
    assert(other.count == 1)
    assert(res == 1)
  }
  test("should able to retry many times when operation fail") {
    val other = new CountService(0)
    val action = guard
      .addAlertService(other)
      .action
      .updateConfig(_.withMaxRetries(3).withExponentialBackoff(1.second).withActionName("retry test"))

    var i = 0
    val op: IO[Int] = IO(
      if (i < 3) { i += 1; throw new Exception }
      else 1)
    val res = action.retry(op).run.unsafeRunSync()
    assert(other.count == 4)
    assert(res == 1)
  }
  test("should fail if can not success in MaxRetries") {
    val other = new CountService(0)
    val action = guard
      .addAlertService(other)
      .action
      .updateConfig(_.withMaxRetries(3).withFullJitter(1.second).withFailOn.withActionName("fail retry "))

    var i = 0
    val op: IO[Int] = IO(
      if (i < 4) { i += 1; throw new Exception }
      else 1)
    assertThrows[Exception](action.retry(op).run.unsafeRunSync())
    assert(other.count == 4)
  }
}
