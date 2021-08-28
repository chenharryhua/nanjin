package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.datetime.DurationFormatter
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.observers.{cloudwatch, console}
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
class ServiceTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("service-level-guard")
    .updateConfig(_.withHostName(HostName.local_host))
    .service("service")
    .updateConfig(_.withConstantDelay(1.seconds).withBrief("ok"))

  test("should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .updateConfig(_.withJitterBackoff(3.second))
      .eventStream(gd =>
        gd("normal-exit-action").trivial.max(10).retry(IO(1)).withFailNotes(_ => null).run.delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStarted])
    assert(d.isInstanceOf[ServiceStopped])
  }

  test("escalate to up level if retry failed") {
    val Vector(s, a, b, c, d, e, f) = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .eventStream { gd =>
        gd("escalate-after-3-time").notice
          .updateConfig(_.withMaxRetries(3).withFibonacciBackoff(0.1.second))
          .run(IO.raiseError(new Exception("oops")))
      }
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetrying])
    assert(c.isInstanceOf[ActionRetrying])
    assert(d.isInstanceOf[ActionRetrying])
    assert(e.isInstanceOf[ActionFailed])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("json codec") {
    val vec = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .eventStream { gd =>
        gd("json-codec").notice
          .updateConfig(_.withMaxRetries(3).withConstantDelay(0.1.second))
          .run(IO.raiseError(new Exception("oops")))
      }
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()
  }

  test("should receive at least 3 report event") {
    val s :: b :: c :: d :: rest = guard
      .updateConfig(_.withReportingSchedule(1.second))
      .eventStream(_.trivial.retry(IO.never).run)
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(b.isInstanceOf[MetricsReport])
    assert(c.isInstanceOf[MetricsReport])
    assert(d.isInstanceOf[MetricsReport])
  }

  test("normal service stop after two operations") {
    val Vector(s, a, b, c, d, e) = guard
      .eventStream(gd => gd("a").notice.retry(IO(1)).run >> gd("b").notice.retry(IO(2)).run)
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStarted])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucced])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionSucced])
    assert(e.isInstanceOf[ServiceStopped])
  }

  test("combine two event streams") {
    val guard = TaskGuard[IO]("two service")
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd => gd("s1-a1").notice.retry(IO(1)).run >> gd("s1-a2").notice.retry(IO(2)).run)
    val ss2 = s2.eventStream(gd => gd("s2-a1").notice.retry(IO(1)).run >> gd("s2-a2").notice.retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucced]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStopped]) == 2)
  }

  test("zoneId ") {
    guard
      .eventStream(ag => IO(assert(ag.zoneId == ag.params.serviceParams.taskParams.zoneId)))
      .compile
      .drain
      .unsafeRunSync()
  }

  ignore("performance") {
    TaskGuard[IO]("performance")
      .service("performance")
      .updateConfig(_.withConstantDelay(1.minute).withReportingSchedule(30.seconds))
      .withJmxReporter(_.inDomain("xyz"))
      .eventStream(ag => ag("performance").trivial.run(IO(0).delayBy(5.second)).foreverM)
      .observe(console(_.show))
      .observe(cloudwatch("test"))
      .interruptAfter(1.hour)
      .compile
      .toList
      .unsafeRunSync()
  }
}
