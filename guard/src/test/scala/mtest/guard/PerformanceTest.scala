package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * 875k/s trace
  *
  * 760k/s silent with Timing and Counting
  *
  * 379k/s aware with Timing and Counting
  *
  * 298k/s notice with Timing and Counting
  *
  * 1151k/s silent
  *
  * 507k/s aware
  *
  * 382k/s notice
  *
  * 351k/s critical with notes
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").updateConfig(_.withMetricReport(cron_1second))
  val take: FiniteDuration = 100.seconds

  def speed(i: Int): String = f"${i / (take.toSeconds * 1000)}%4dk/s"

  ignore("alert") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.alert("alert").info("alert").map(_ => i += 1)
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} alert")
  }

  test("with trace") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("trace").retry(IO(i += 1))
      ag.root("root").use(s => ts.runWithSpan(s).foreverM.timeout(take)).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} trace")
  }

  test("Silent with Timing and Counting") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} silent with Timing and Counting")
  }

  test("aware with Timing and Counting") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.aware.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} aware with Timing and Counting")
  }

  test("notice with Timing and Counting") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.notice.withCounting.withTiming).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} notice with Timing and Counting")
  }

  test("silent") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} silent")
  }

  test("aware") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.aware.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} aware")
  }

  test("notice") {
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.notice.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} notice")
  }

  test("critical with notes") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag
        .action("t", _.critical.withoutTiming.withoutCounting)
        .retry((_: Int) => IO(i += 1))
        .logOutput((i, _) => Json.fromInt(i))

      ts.run(1).foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical with notes")
  }
}
