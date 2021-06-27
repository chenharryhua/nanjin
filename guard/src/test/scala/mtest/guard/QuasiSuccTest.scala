package mtest.guard

import cats.data.Chain
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.alert.{
  ActionFailed,
  ActionQuasiSucced,
  ConsoleService,
  LogService,
  MetricsService,
  ServicePanic,
  ServiceStopped,
  SlackService
}
import fs2.Chunk
import org.scalatest.funsuite.AnyFunSuite

import java.time.{Duration => JavaDuration}
import scala.concurrent.duration._
class QuasiSuccTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("qusai succ app").service("quasi")
  val logging =
    SlackService(SimpleNotificationService.fake[IO]) |+| MetricsService[IO](new MetricRegistry()) |+| LogService[
      IO] |+| ConsoleService[IO]

  def f(a: Int): IO[Int] = IO(100 / a)

  test("all succ - list") {
    val Vector(a, b) = guard
      .eventStream(action => action("all-good").quasi(List(1, 2, 3))(f).seqRun)
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
          .seqRun)
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
            .seqRun)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionQuasiSucced].numSucc == 2)
    assert(a.asInstanceOf[ActionQuasiSucced].errors.size == 1)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("partial succ - vector") {
    def f(a: Int): IO[Int] = IO.sleep(1.second) >> IO(100 / a)
    val Vector(a, b) =
      guard
        .eventStream(action =>
          action("partial-good")
            .quasi(Vector(0, 0, 1, 1))(f)
            .withFailNotes(_.map(n => s"${n._1} --> ${n._2.id}").mkString("\n"))
            .seqRun)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    val succ = a.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 2)
    assert(succ.errors.size == 2)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds > 3)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("parallel") {
    def f(a: Int): IO[Int] = IO.sleep(1.second) >> IO(100 / a)
    val Vector(a, b) =
      guard
        .eventStream(action => action("parallel").quasi(Vector(0, 0, 0, 1, 1, 1))(f).parRun)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    val succ = a.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 3)
    assert(succ.errors.size == 3)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds < 2)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("pure actions") {
    def f(a: Int): IO[Unit] = IO.sleep(1.second) <* IO(100 / a)
    val Vector(a, b) =
      guard
        .eventStream(action => action("pure actions").quasi(f(0), f(0), f(0), f(1), f(1), f(1)).parRun)
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    val succ = a.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 3)
    assert(succ.errors.size == 3)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds < 2)
    assert(b.isInstanceOf[ServiceStopped])
  }

  test("cancallation - internal") {
    def f(a: Int): IO[Unit] = IO.sleep(1.second) <* IO(100 / a)
    val Vector(a, b) =
      guard
        .eventStream(action =>
          action("internal-cancel").quasi(f(0), IO.sleep(1.second) >> IO.canceled, f(1), f(2)).seqRun)
        .observe(_.evalMap(logging.alert).drain)
        .interruptAfter(5.seconds)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionFailed].error.message == "ActionCanceledInternally: internal-cancel")
    assert(b.isInstanceOf[ServicePanic])
  }

  test("cancallation - external") {
    def f(a: Int): IO[Unit] = IO.sleep(1.second) <* IO(100 / a)
    val Vector(a, b) =
      guard
        .eventStream(action =>
          IO.parSequenceN(2)(
            List(action("external-cancel").quasi(Vector(f(0), f(1))).seqRun, IO.sleep(1.second) >> IO.canceled)))
        .observe(_.evalMap(logging.alert).drain)
        .compile
        .toVector
        .unsafeRunSync()
    assert(a.asInstanceOf[ActionFailed].error.message == "ActionCanceledExternally: external-cancel")
    assert(b.isInstanceOf[ServiceStopped])
  }
}
