package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionQuasiSucced,
  LogService,
  MetricsService,
  ServiceStopped,
  SlackService
}
import org.scalatest.funsuite.AnyFunSuite
import cats.syntax.all._

class QuasiSuccTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("qusai succ app").service("quasi")
  val logging =
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[IO]

  test("all succ") {
    val ls: List[Either[Throwable, Int]] = List(Right(1), Right(2), Right(3))
    val Vector(a, b) = guard
      .eventStream(action => action("all-good").quasi(IO(ls)))
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 3)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.isEmpty)
    assert(b.isInstanceOf[ServiceStopped])
  }
  test("all fail") {
    val ls: List[Either[Throwable, Int]] = List(Left(new Exception), Left(new Exception))
    // val f =
    val Vector(a, b) = guard
      .eventStream(action => action("all-fail").quasi(IO(ls)))
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 0)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 2)
    assert(b.isInstanceOf[ServiceStopped])
  }
  test("partial succ") {
    def f(a: Int): IO[Int] = IO(100 / a)
    val Vector(a, b) =
      guard
        .eventStream(action => action("partial-good").quasi(List(2, 0, 1))(f))
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 2)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 1)
    assert(b.isInstanceOf[ServiceStopped])
  }
}
