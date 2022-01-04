package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service = TaskGuard[IO]("performance").service("actions")
  val take    = 10.seconds

  test("critical") {
    var i = 0
    service.eventStream { ag =>
      ag.span("c").critical.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i critical")
  }

  test("notice") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("n").notice.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i notice")
  }

  test("normal") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("m").normal.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i normal")
  }

  test("trivial") {
    var i = 0
    service.eventStream { ag =>
      ag.span("t").trivial.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i trivial")
  }

  test("critical - no timing") {
    var i = 0
    service.eventStream { ag =>
      ag.span("c").critical.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i critical - no timing")
  }

  test("notice - no timing") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("n").notice.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i notice - no timing")
  }

  test("normal - no timing") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("m").normal.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i normal - no timing")
  }

  test("trivial - no timing") {
    var i = 0
    service.eventStream { ag =>
      ag.span("t").trivial.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i trivial - no timing")
  }

}
