package mtest.guard

import cats.data.Chain
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
import fs2.Chunk

class QuasiSuccTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("qusai succ app").service("quasi")
  val logging =
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[IO]

  def f(a: Int): IO[Int] = IO(100 / a)

  test("all succ") {
    val ls: List[Int] = List(1, 2, 3)
    val Vector(a, b) = guard
      .eventStream(action => action("all-good").quasi(ls)(f).withSuccNotes(_ => "succ").run)
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 3)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.isEmpty)
    assert(b.isInstanceOf[ServiceStopped])
  }
  test("all fail") {
    val ls: Chunk[Int] = Chunk(0, 0, 0)
    val Vector(a, b) = guard
      .eventStream(action =>
        action("all-fail")
          .updateConfig(_.withSuccAlertOn.withFailAlertOff)
          .quasi(ls)(f)
          .withFailNotes(_ => "failure")
          .run)
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 0)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 3)
    assert(b.isInstanceOf[ServiceStopped])
  }
  test("partial succ") {

    val Vector(a, b) =
      guard
        .eventStream(action => action("partial-good").quasi(Chain(2, 0, 1))(f).withFailNotes(_ => "quasi succ").run)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 2)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 1)
    assert(b.isInstanceOf[ServiceStopped])
  }
}
