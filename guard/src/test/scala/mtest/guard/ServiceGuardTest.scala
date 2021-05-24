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
    TaskGuard[IO]("ServiceTest")
      .addAlertService(SlackService(SimpleNotificationService.fake[IO]))
      .addAlertService(new ExceptionService)

  test("should raise abnormal stop signal when service is not designed for long-run") {
    val count = new AbnormalAlertService(0)
    val service = guard
      .addAlertService(count)
      .service("abnormal test")
      .updateConfig(
        _.withFibonacciBackoff(1.second)
          .withExponentialBackoff(1.second)
          .withConstantDelay(1.second)
          .withFullJitter(1.second))
    service.run(Stream(1).covary[IO]).compile.drain.unsafeRunTimed(5.seconds)
    assert(count.count > 2)
  }

  test("service should recover its self - may fail occasionally because of the randomness.") {
    val count = new PanicAlertService(0)
    val service =
      guard
        .addAlertService(count)
        .service("recovery test")
        .updateConfig(_.withConstantDelay(0.5.second).withHealthCheckInterval(1.second))
    service
      .run(IO(if (Random.nextBoolean()) throw new Exception else 1).delayBy(0.5.second).void.foreverM)
      .unsafeRunTimed(5.seconds)
    assert((count.count > 2))
  }
}
