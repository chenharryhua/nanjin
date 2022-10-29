package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Ignore

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * 317k/s critical
  *
  * 606k/s silent
  *
  * 281k/s critical with notes
  *
  * 510k/s trivial with Timing and Counting
  *
  * 264k/s notice with Timing and Counting
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").updateConfig(_.withMetricReport(secondly))
  val take: FiniteDuration = 100.seconds

  def speed(i: Int): String = s"${i / (take.toSeconds * 1000)}k/s"

  ignore("alert") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.alert("alert").info("alert").map(_ => i += 1)
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} alert")
  }

//  test("trace") {
//    var i: Int = 0
//    service.eventStream { ag =>
//      val ts = ag.trace("trace", _.silent.withoutTiming.withoutCounting).use(_.retry(IO(i += 1)).run)
//      ts.foreverM.timeout(take).attempt
//    }.compile.drain.unsafeRunSync()
//    println(s"${speed(i)} trace")
//  }

  test("critical") {
    var i = 0
    service.eventStream { ag =>
      val ts =
        ag.action("t", _.critical.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical")
  }

  test("silent") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.silent.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} silent")
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

  test("trivial with Timing and Counting") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.trivial.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} trivial with Timing and Counting")
  }

  test("notice notice with Timing and Counting") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.action("t", _.notice.withTiming.withCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} notice with Timing and Counting")
  }

}
