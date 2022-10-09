package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxMonadErrorRethrow
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.guard.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.NJEvent.*
import com.github.chenharryhua.nanjin.guard.observers.console
import eu.timepit.refined.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.util.control.ControlThrowable
import scala.util.Try

class ServiceTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("service-level-guard")
    .updateConfig(_.withHostName(HostName.local_host).withHomePage("https://abc.com/efg"))
    .service("service")
    .updateConfig(_.withConstantDelay(1.seconds).withBrief("test"))

  test("1.should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .updateConfig(_.withJitterBackoff(3.second).withMetricReport(24.hours))
      .updateConfig(
        _.withQueueCapacity(1)
          .withMetricReset("*/30 * * ? * *")
          .withMetricDailyReset
          .withMetricMonthlyReset
          .withMetricWeeklyReset)
      .eventStream(gd => gd.action("t", _.silent).retry(Try(1)).logOutput(_ => null).run.delayBy(1.second))
      .map(e => decode[NJEvent](e.asJson.noSpaces).toOption)
      .unNone
      .compile
      .toVector
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("2.escalate to up level if retry failed") {
    val Vector(s, a, b, c, d, e, f) = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .updateConfig(_.withQueueCapacity(2))
      .eventStream { gd =>
        gd.action("t", _.notice.withFibonacciBackoff(0.1.second, 3))
          .retry(IO.raiseError(new Exception("oops")))
          .run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionRetry])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
  }

  test("3.should throw exception when fatal error occurs") {
    val List(a, b, c, d) = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .updateConfig(_.withQueueCapacity(2))
      .eventStream { gd =>
        gd.action("t", _.notice.withFibonacciBackoff(0.1.second, 3))
          .retry(IO.raiseError(new ControlThrowable("fatal error") {}))
          .run
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .evalTap(console.simple[IO])
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionFail])
    assert(d.isInstanceOf[ServiceStop])
  }

  test("4.json codec") {
    val a :: b :: c :: d :: e :: f :: g :: _ = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .updateConfig(_.withQueueCapacity(3))
      .eventStream { gd =>
        gd.action("t", _.notice.withConstantDelay(0.1.second, 3)).retry(Left(new Exception("oops"))).run

      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ServicePanic])
  }

  test("5.should receive at least 3 report event") {
    val s :: b :: c :: d :: _ = guard
      .updateConfig(_.withMetricReport(1.second))
      .updateConfig(_.withQueueCapacity(4))
      .eventStream(_.action("t", _.silent).retry(IO.never).run)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .interruptAfter(5.second)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReport])
    assert(c.isInstanceOf[MetricReport])
    assert(d.isInstanceOf[MetricReport])
  }

  test("6.force reset") {
    val s :: b :: c :: _ = guard
      .updateConfig(_.withMetricReport(1.second))
      .updateConfig(_.withQueueCapacity(4))
      .eventStream(ag => ag.metrics.reset >> ag.metrics.reset)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toList
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[MetricReset])
    assert(c.isInstanceOf[MetricReset])
  }

  test("7.normal service stop after two operations") {
    val Vector(s, a, b, c, d, e) = guard
      .updateConfig(_.withQueueCapacity(10))
      .eventStream(gd =>
        gd.action("t", _.notice).retry(Try(1)).run >> gd.action("t", _.notice).retry(IO(2)).run)
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toVector
      .unsafeRunSync()

    assert(s.isInstanceOf[ServiceStart])
    assert(a.isInstanceOf[ActionStart])
    assert(b.isInstanceOf[ActionSucc])
    assert(c.isInstanceOf[ActionStart])
    assert(d.isInstanceOf[ActionSucc])
    assert(e.isInstanceOf[ServiceStop])
  }

  test("8.combine two event streams") {
    val guard = TaskGuard[IO]("two service")
    val s1    = guard.service("s1")
    val s2    = guard.service("s2")

    val ss1 = s1.eventStream(gd =>
      gd.action("t", _.notice).retry(IO(1)).run >> gd.action("t", _.notice).retry(IO(2)).run)
    val ss2 = s2.eventStream(gd =>
      gd.action("t", _.notice).retry(IO(1)).run >> gd.action("t", _.notice).retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucc]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStop]) == 2)
  }

  test("9.print agent params") {
    guard.eventStream(ag => IO.println(ag.zoneId)).compile.drain.unsafeRunSync()
  }

  test("10. multiple service restart") {
    val a :: b :: c :: d :: e :: f :: g :: h :: i :: _ = guard
      .updateConfig(_.withConstantDelay(1.second))
      .eventStream(_.action("oops", _.silent).retry(IO.raiseError[Int](new Exception("oops"))).run)
      .interruptAfter(5.seconds)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionFail])
    assert(c.isInstanceOf[ServicePanic])
    assert(d.isInstanceOf[ServiceStart])
    assert(e.isInstanceOf[ActionFail])
    assert(f.isInstanceOf[ServicePanic])
    assert(g.isInstanceOf[ServiceStart])
    assert(h.isInstanceOf[ActionFail])
    assert(i.isInstanceOf[ServicePanic])

  }

  test("11.should give up") {

    val List(a, b, c, d, e, f, g) = guard
      .updateConfig(_.withAlwaysGiveUp)
      .eventStream { gd =>
        gd.action("t", _.notice.withFibonacciBackoff(0.1.second, 3)).retry(IO.raiseError(new Exception)).run
      }
      .debug()
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ActionStart])
    assert(c.isInstanceOf[ActionRetry])
    assert(d.isInstanceOf[ActionRetry])
    assert(e.isInstanceOf[ActionRetry])
    assert(f.isInstanceOf[ActionFail])
    assert(g.isInstanceOf[ServiceStop])
  }

//  test("12.dummy agent should not block") {
//    val dummy = TaskGuard.dummyAgent[IO]
//    dummy.use(_.action("t", _.critical).retry(IO(1)).run.replicateA(10)).unsafeRunSync()
//  }
}
