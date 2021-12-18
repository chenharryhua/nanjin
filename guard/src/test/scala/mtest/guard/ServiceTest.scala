package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.aws.SimpleNotificationService
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.datetime.{crontabs, DurationFormatter}
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.observers.{logging, slack}
import cron4s.lib.javatime.javaTemporalInstance
import io.circe.parser.decode
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZonedDateTime
import scala.concurrent.duration.*

class ServiceTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("service-level-guard")
    .updateConfig(_.withHostName(HostName.local_host))
    .service("service")
    .updateConfig(_.withConstantDelay(1.seconds))

  test("should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .updateConfig(_.withJitterBackoff(3.second))
      .updateConfig(_.withQueueCapacity(1))
      .eventStream(gd =>
        gd.span("normal-exit-action").trivial.max(10).retry(IO(1)).withFailNotes(_ => null).run.delayBy(1.second))
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
      .updateConfig(_.withQueueCapacity(2))
      .eventStream { gd =>
        gd.span("escalate-after-3-time")
          .notice
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
      .updateConfig(_.withQueueCapacity(3))
      .eventStream { gd =>
        gd.span("json-codec")
          .notice
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
      .updateConfig(_.withMetricSchedule(1.second))
      .updateConfig(_.withQueueCapacity(4))
      .eventStream(_.trivial.retry(IO.never).run)
      .debug()
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
      .updateConfig(_.withQueueCapacity(10))
      .eventStream(gd => gd.span("a").notice.retry(IO(1)).run >> gd.span("b").notice.retry(IO(2)).run)
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

    val ss1 = s1.eventStream(gd => gd.span("s1-a1").notice.retry(IO(1)).run >> gd.span("s1-a2").notice.retry(IO(2)).run)
    val ss2 = s2.eventStream(gd => gd.span("s2-a1").notice.retry(IO(1)).run >> gd.span("s2-a2").notice.retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucced]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStopped]) == 2)
  }

  test("slack") {
    TaskGuard[IO]("slack")
      .service("slack")
      .updateConfig(_.withConstantDelay(1.hour).withMetricSchedule(crontabs.secondly).withQueueCapacity(20))
      .eventStream { root =>
        val ag = root.span("slack").max(1).critical.updateConfig(_.withConstantDelay(2.seconds))
        ag.run(IO(1)) >> ag.alert("notify").error("error.msg") >> ag.run(IO.raiseError(new Exception("oops"))).attempt
      }
      .evalTap(logging[IO](_.show))
      .through(slack[IO](SimpleNotificationService.fake[IO]).at("@chenh"))
      .compile
      .drain
      .unsafeRunSync()
  }
}
