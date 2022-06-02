package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.service.ServiceGuard
import eu.timepit.refined.auto.*
import io.circe.syntax.EncoderOps
import org.scalatest.Ignore
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.PerformanceTest"

/** last time: (run more than once, pick up the best)
  *
  * 217k/s critical
  *
  * 247k/s critical - notes
  *
  * 231k/s critical - expensive notes
  *
  * 248k/s notice
  *
  * 444k/s normal
  *
  * 397k/s - normal expensive
  *
  * 434k/s trivial
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance")
      .service("actions")
      .updateConfig(_.withQueueCapacity(30).withMetricReport(10.seconds))
  val take: FiniteDuration = 100.seconds

  def speed(i: Int): String = s"${i / (take.toSeconds * 1000)}k/s"

  test("critical") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.span("c").critical.updateConfig(_.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical")
  }

  test("critical - notes") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag
        .span("cn")
        .critical
        .updateConfig(_.withoutTiming.withoutCounting)
        .retry(IO(i += 1))
        .logOutput(_.asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical - notes")
  }

  test("critical - expensive notes") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag
        .span("cen")
        .critical
        .updateConfig(_.withoutTiming.withoutCounting)
        .expensive
        .retry(IO(i += 1))
        .logOutput(_.asJson)
        .run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} critical - expensive notes")
  }

  test("notice") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.span("nt").notice.updateConfig(_.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} notice")
  }

  test("normal") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts = ag.span("n").normal.updateConfig(_.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} normal")
  }

  test("normal - expensive") {
    var i: Int = 0
    service.eventStream { ag =>
      val ts =
        ag.span("ne").normal.updateConfig(_.withoutTiming.withoutCounting).expensive.retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} - normal expensive")
  }

  test("trivial") {
    var i = 0
    service.eventStream { ag =>
      val ts = ag.span("t").trivial.updateConfig(_.withoutTiming.withoutCounting).retry(IO(i += 1)).run
      ts.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"${speed(i)} trivial")
  }
}
