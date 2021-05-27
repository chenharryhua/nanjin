package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard._
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration._
import scala.util.Random

final class AbnormalAlertService(var abnormal: Int, var healthCheck: Int) extends AlertService[IO] {

  override def alert(event: Event): IO[Unit] = event match {
    case a: ServiceAbnormalStop => IO.println(a) >> IO(abnormal += 1)
    case h: ServiceHealthCheck  => IO.println(h) >> IO(healthCheck += 1)
    case x                      => IO.println(x)
  }
}

final class PanicAlertService(var panic: Int, var started: Int) extends AlertService[IO] {

  override def alert(event: Event): IO[Unit] = event match {
    case p: ServicePanic   => IO.println(p) >> IO(panic += 1)
    case h: ServiceStarted => IO.println(h) >> IO(started += 1)
    case x                 => IO.println(x)
  }
}

class ServiceGuardTest extends AnyFunSuite {

  val guard: TaskGuard[IO] =
    TaskGuard[IO]("ServiceTest")

//  test("should raise abnormal stop signal when service is not designed for long-run") {
//    val count = new AbnormalAlertService(0, 0)
//    val service = guard
//      .addAlertService(count)
//      .service("abnormal test")
//      .updateConfig(_.withConstantDelay(1.second).withHealthCheckInterval(1.second).withHealthCheckDisabled)
//    service.run(Stream(1).covary[IO]).compile.drain.unsafeRunTimed(5.seconds)
//    assert(count.abnormal == 1)
//    assert(count.healthCheck == 0)
//  }

//  ignore("should raise abnormal stop signal when service is not designed for long-run. health-check was kicked off") {
//    val count = new AbnormalAlertService(0, 0)
//    val service = guard
//      .addAlertService(count)
//      .service("abnormal test")
//      .updateConfig(_.withConstantDelay(1.second).withHealthCheckInterval(1.second))
//    service.run(IO(1).delayBy(3.seconds)).debug().compile.drain.unsafeRunTimed(5.seconds)
//    assert(count.abnormal == 1)
//    assert(count.healthCheck >= 1)
//  }

  test("service should recover its self.") {
    val count = new PanicAlertService(0, 0)
    val service =
      guard.service("recovery test").updateConfig(_.withConstantDelay(3.second).withHealthCheckInterval(6.second))

    var i = 0
    val run =
      service.run(gd =>
        (IO(
          if (i < 5) { i += 1; throw new Exception }
          else { i += 1; i })
          .flatMap(x =>
            gd("abc")
              .updateConfig(_.failOn.withConstantDelay(1.second))
              .retry(IO(if (Random.nextBoolean()) throw new Exception else 1))
              .run >> gd("efg").retry(IO(1)).run)
          .delayBy(0.5.second)
          .void
          .foreverM))
    run.debug().compile.drain.unsafeRunTimed(150.seconds)
    assert(count.panic == 2)
    assert(count.started == 1)
  }
}
