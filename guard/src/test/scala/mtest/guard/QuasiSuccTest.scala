package mtest.guard

import cats.data.Chain
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionQuasiSucced,
  ConsoleService,
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
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[
      IO] |+| ConsoleService[IO]

  def f(a: Int): IO[Int] = IO(100 / a)

  test("all succ - list") {
    val Vector(a, b) = guard
      .eventStream(action => action("all-good").quasi(List(1, 2, 3))(f).run)
      .observe(_.evalMap(logging.alert).drain)
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 3)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.isEmpty)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("all fail - chunk") {
    val Vector(a, b) = guard
      .eventStream(action =>
        action("all-fail")
          .updateConfig(_.withSuccAlertOn.withFailAlertOff)
          .quasi(Chunk(0, 0, 0))(f)
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

  test("partial succ - chain") {
    val Vector(a, b) =
      guard
        .eventStream(action =>
          action("partial-good")
            .quasi(Chain(2, 0, 1))(f)
            .withFailNotes(_ => "quasi succ")
            .withSuccNotes(_ => "succ")
            .run)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 2)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 1)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("partial succ - vector") {
    val Vector(a, b) =
      guard
        .eventStream(action =>
          action("partial-good")
            .quasi(Vector(0, 0, 1))(f)
            .withFailNotes(_.map(n => s"${n._1} --> ${n._2.id}").mkString("\n"))
            .run)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 1)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 2)
    assert(b.isInstanceOf[ServiceStopped])
  }
}
