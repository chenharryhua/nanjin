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

/** last time:
  *
  * 89854651 critical - no timing
  *
  * 87683264 notice - expensive no timing
  *
  * 100883475 normal - expensive no timing
  *
  * 106398872 trivial - no timing
  */

@Ignore
class PerformanceTest extends AnyFunSuite {
  val service: ServiceGuard[IO] =
    TaskGuard[IO]("performance").service("actions").updateConfig(_.withQueueCapacity(50).withMetricReport(10.seconds))
  val take: FiniteDuration = 100.seconds
  val repeat               = 1

  test("critical") {
    var i = 0
    val run = service.eventStream { ag =>
      ag.span("critical").critical.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.filter(_.isInstanceOf[MetricReport]).evalTap(IO.println).compile.drain
    run.replicateA(repeat).unsafeRunSync()
  }

  test("notice") {
    var i = 0
    val run = service.eventStream { ag =>
      ag.span("notice").notice.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.filter(_.isInstanceOf[MetricReport]).evalTap(IO.println).compile.drain
    run.replicateA(repeat).unsafeRunSync()
  }

  test("normal") {
    var i = 0
    val run = service.eventStream { ag =>
      ag.span("normal").normal.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.filter(_.isInstanceOf[MetricReport]).evalTap(IO.println).compile.drain
    run.replicateA(repeat).unsafeRunSync()
  }

  test("trivial") {
    var i = 0
    val run = service.eventStream { ag =>
      ag.span("trivial").trivial.retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.filter(_.isInstanceOf[MetricReport]).evalTap(IO.println).compile.drain
    run.replicateA(repeat).unsafeRunSync()
  }

  test("critical - no timing") {
    var i = 0
    service.eventStream { ag =>
      ag.span("c").critical.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i critical - no timing")
  }

  test("notice - expensive no timing") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("n").notice.expensive.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i notice - expensive no timing")
  }

  test("normal - expensive no timing") {
    var i: Int = 0
    service.eventStream { ag =>
      ag.span("m").normal.expensive.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i normal - expensive no timing")
  }

  test("trivial - no timing") {
    var i = 0
    service.eventStream { ag =>
      ag.span("t").trivial.updateConfig(_.withoutTiming).retry(IO(i += 1)).run.foreverM.timeout(take).attempt
    }.compile.drain.unsafeRunSync()
    println(s"$i trivial - no timing")
  }
}
