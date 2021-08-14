package mtest.guard

import cats.data.Chain
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.observers.{jsonConsole, showLog, slack}
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Chunk
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration as JavaDuration
import scala.concurrent.duration.*

class QuasiSuccTest extends AnyFunSuite {
  val guard = TaskGuard[IO]("qusai succ app").service("quasi")

  def f(a: Int): IO[Int] = IO(100 / a)

  test("quasi all succ - list") {
    val Vector(s, a, b, c) =
      guard.eventStream(action => action("all-good").quasi(List(1, 2, 3))(f).seqRun).compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionQuasiSucced].numSucc == 3)
    assert(b.asInstanceOf[ActionQuasiSucced].errors.isEmpty)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi all fail - chunk") {
    val Vector(s, a, b, c) = guard
      .eventStream(action => action("all-fail").quasi(Chunk(0, 0, 0))(f).withFailNotes(_ => "failure").seqRun)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionQuasiSucced].numSucc == 0)
    assert(b.asInstanceOf[ActionQuasiSucced].errors.size == 3)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi partial succ - chain") {
    val sns = SimpleNotificationService.fake[IO]
    val Vector(s, a, b, c) =
      guard
        .eventStream(action =>
          action("partial-good")
            .quasi(Chain(2, 0, 1))(f)
            .withFailNotes(_ => "quasi succ")
            .withSuccNotes(_ => "succ")
            .seqRun)
        .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
        .unNone
        .observe(showLog)
        .observe(jsonConsole)
        .observe(slack(sns))
        .compile
        .toVector
        .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionQuasiSucced].numSucc == 2)
    assert(b.asInstanceOf[ActionQuasiSucced].errors.size == 1)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi partial succ - vector") {
    def f(a: Int): IO[Int] = IO.sleep(1.second) >> IO(100 / a)
    val Vector(s, a, b, c) =
      guard
        .eventStream(action =>
          action("partial-good")
            .quasi(Vector(0, 0, 1, 1))(f)
            .withFailNotes(_.map(n => s"${n._1} --> ${n._2.id}").mkString("\n"))
            .seqRun)
        .compile
        .toVector
        .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    val succ = b.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 2)
    assert(succ.errors.size == 2)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds > 3)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi parallel - par") {
    def f(a: Int): IO[Int] = IO.sleep(1.second) >> IO(100 / a)
    val Vector(s, a, b, c) =
      guard
        .eventStream(action => action("parallel").quasi(Vector(0, 0, 0, 1, 1, 1))(f).parRun)
        .compile
        .toVector
        .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    val succ = b.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 3)
    assert(succ.errors.size == 3)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds < 2)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi parallel - parN") {
    def f(a: Int): IO[Int] = IO.sleep(1.second) >> IO(100 / a)
    val Vector(s, a, b, c) =
      guard
        .eventStream(action => action("parallel").quasi(Vector(0, 0, 0, 1, 1, 1))(f).parRun(3))
        .compile
        .toVector
        .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    val succ = b.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 3)
    assert(succ.errors.size == 3)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds < 3)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi pure actions") {
    def f(a: Int): IO[Unit] = IO.sleep(1.second) <* IO(100 / a)
    val Vector(s, a, b, c) =
      guard
        .eventStream(action => action("pure actions").quasi(f(0), f(0), f(0), f(1), f(1), f(1)).parRun)
        .compile
        .toVector
        .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    val succ = b.asInstanceOf[ActionQuasiSucced]
    assert(succ.numSucc == 3)
    assert(succ.errors.size == 3)
    assert(JavaDuration.between(succ.actionInfo.launchTime, succ.timestamp).abs.getSeconds < 2)
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi cancallation - internal") {
    def f(a: Int): IO[Unit] = IO.sleep(1.second) <* IO(100 / a)
    val Vector(s, a, b, c) =
      guard
        .eventStream(action =>
          action("internal-cancel").quasi(f(0), IO.sleep(1.second) >> IO.canceled, f(1), f(2)).seqRun)
        .interruptAfter(5.seconds)
        .compile
        .toVector
        .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.asInstanceOf[ActionFailed].error.throwable.get.getMessage == "action was canceled internally")
    assert(c.isInstanceOf[ServicePanic])
  }

  test("quasi cancallation - external (could be wrong)") {
    def f(a: Int): IO[Unit] = IO.sleep(1.second) <* IO(100 / a)
    val Vector(s, a, b, c) =
      guard.eventStream { action =>
        val a1 = action("external-cancel").quasi(Vector(f(0), f(1))).seqRun
        val a2 = IO.canceled
        List(a1.delayBy(3.seconds), a2.delayBy(1.second)).parSequence_
      }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionQuasiSucced])
    assert(c.isInstanceOf[ServiceStopped])
  }

  test("quasi multi-layers seq") {
    val Vector(s, a, b, c, d, e, f, g, h, i, j, k, l) =
      guard.eventStream { action =>
        val a1 = action("compute1").run(IO(1))
        val a2 = action("exception")
          .updateConfig(_.withConstantDelay(1.second).withMaxRetries(3))
          .run(IO.raiseError[Int](new Exception))
        val a3 = action("compute2").run(IO(2))
        action("quasi")
          .quasi(a1, a2, a3)
          .withSuccNotes(_.map(_.toString).mkString)
          .withFailNotes(_.map(_.message).mkString)
          .seqRun
      }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.asInstanceOf[ActionStart].actionInfo.actionName == "quasi")
    assert(b.asInstanceOf[ActionStart].actionInfo.actionName == "compute1")
    assert(c.asInstanceOf[ActionSucced].actionInfo.actionName == "compute1")
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionRetrying])
    assert(f.isInstanceOf[ActionRetrying])
    assert(g.isInstanceOf[ActionRetrying])
    assert(h.isInstanceOf[ActionFailed])
    assert(i.isInstanceOf[ActionStart])
    assert(j.asInstanceOf[ActionSucced].actionInfo.actionName == "compute2")
    assert(k.isInstanceOf[ActionQuasiSucced])
    assert(l.isInstanceOf[ServiceStopped])
  }

  test("quasi multi-layers - par") {
    val Vector(s, a, b, c, d, e, f, g, h, i, j, k, l) =
      guard.eventStream { action =>
        val a1 = action("compute1").run(IO.sleep(5.seconds) >> IO(1))
        val a2 =
          action("exception").max(3).updateConfig(_.withConstantDelay(1.second)).run(IO.raiseError[Int](new Exception))
        val a3 = action("compute2").run(IO.sleep(5.seconds) >> IO(2))
        action("quasi")
          .quasi(a1, a2, a3)
          .withSuccNotes(_.map(_.toString).mkString)
          .withFailNotes(_.map(_.message).mkString)
          .parRun(3)
      }.compile.toVector.unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionStart])
    assert(e.isInstanceOf[ActionRetrying])
    assert(f.isInstanceOf[ActionRetrying])
    assert(g.isInstanceOf[ActionRetrying])
    assert(h.isInstanceOf[ActionFailed])
    assert(i.isInstanceOf[ActionSucced])
    assert(j.isInstanceOf[ActionSucced])
    assert(k.isInstanceOf[ActionQuasiSucced])
    assert(l.isInstanceOf[ServiceStopped])
  }
}
