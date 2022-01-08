package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.MetricReport
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * 22132321 critical
  *
  * 23960750 critical - notes
  *
  * 20128671 critical - expensive notes
  *
  * 23187371 notice
  *
  * 42019615 normal
  *
  * 38032930 normal - expensive
  *
  * 42671655 trivial
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").updateConfig(_.withQueueCapacity(50).withMetricReport(10.seconds))
  val take: FiniteDuration = 100.seconds

  test("critical") {
    var i = 0
    service.eventStream { ag =>
      ag.span("c")
        .critical
        .updateConfig(_.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i critical")
  }

  test("critical - notes") {
    var i = 0
    service.eventStream { ag =>
      ag.span("cn")
        .critical
        .updateConfig(_.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .withSuccNotes(_ => "ok")
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i critical - notes")
  }

  test("critical - expensive notes") {
    var i = 0
    service.eventStream { ag =>
      ag.span("cen")
        .critical
        .updateConfig(_.withoutTiming.withoutCounting)
        .expensive
        .retry(IO(i += 1))
        .withSuccNotes(_ => "ok")
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i critical - expensive notes")
  }

  test("notice") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("nt")
        .notice
        .updateConfig(_.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i notice")
  }

  test("normal") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("n")
        .normal
        .updateConfig(_.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i normal")
  }

  test("normal - expensive") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("ne")
        .normal
        .updateConfig(_.withoutTiming.withoutCounting)
        .expensive
        .retry(IO(i += 1))
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i normal - expensive")
  }

  test("trivial") {
    var i = 0
    service.eventStream { ag =>
      ag.span("t")
        .trivial
        .updateConfig(_.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .run
        .foreverM
        .timeout(take)
        .attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i trivial")
  }
}
