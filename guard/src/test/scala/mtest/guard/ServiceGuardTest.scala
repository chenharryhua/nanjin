package mtest.guard

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Sync}
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.util.Random

final class AbnormalAlertService(var count: Int) extends AlertService[IO] {

  override def alert(status: Status)(implicit F: Sync[IO]): IO[Unit] = status match {
    case _: ServiceAbnormalStop => IO(count += 1)
    case _                      => IO.unit
  }
}

final class PanicAlertService(var count: Int) extends AlertService[IO] {

  override def alert(status: Status)(implicit F: Sync[IO]): IO[Unit] = status match {
    case _: ServicePanic => IO(count += 1)
    case _               => IO.unit
  }
}

class ServiceGuardTest extends AnyFunSuite {

  val guard: TaskGuard[IO] =
    TaskGuard[IO]
      .addAlertService(SlackService(SimpleNotificationService.fake[IO]))
      .addAlertService(new ExceptionService)
      .withApplicationName("ServiceTest")

  test("should raise abnormal stop signal when service is not designed for long-run") {
    val count = new AbnormalAlertService(0)
    val service = guard
      .addAlertService(count)
      .service
      .updateConfig(
        _.fibonacciBackoff(1.second)
          .exponentialBackoff(1.second)
          .constantDelay(1.second)
          .fullJitter(1.second)
          .withServiceName("abnormal test"))
    service.run(Stream(1).covary[IO]).compile.drain.unsafeRunTimed(5.seconds)
    assert(count.count > 2)
  }

  test("service should recover its self") {
    val count = new PanicAlertService(0)
    val service =
      guard
        .addAlertService(count)
        .service
        .updateConfig(_.constantDelay(0.5.second).withHealthCheckInterval(1.second).withServiceName("recovery test"))
    service
      .run(IO(if (Random.nextBoolean()) throw new Exception else 1).delayBy(0.5.second).foreverM)
      .unsafeRunTimed(5.seconds)
    assert((count.count > 2))
  }
}
