package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxMonadErrorRethrow
import com.github.chenharryhua.nanjin.common.HostName
import com.github.chenharryhua.nanjin.common.guard.Name
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

class ServiceTest extends AnyFunSuite {

  val guard = TaskGuard[IO]("service-level-guard")
    .updateConfig(_.withHostName(HostName.local_host).withHomePage("https://abc.com/efg"))
    .service("service")
    .updateConfig(_.withConstantDelay(1.seconds).withBrief("test"))

  test("1.should stopped if the operation normally exits") {
    val Vector(a, d) = guard
      .updateConfig(_.withJitterBackoff(3.second).withMetricReport("*/30 * * ? * *"))
      .updateConfig(_.withQueueCapacity(1).withMetricDailyReset.withMetricMonthlyReset.withMetricWeeklyReset
        .withMetricReset("*/30 * * ? * *"))
      .eventStream(gd =>
        gd.span("normal-exit-action").retry(IO(1)).logOutput(_ => null).run.delayBy(1.second))
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
        gd.span("escalate-after-3-time")
          .notice
          .updateConfig(_.withFibonacciBackoff(0.1.second, 3))
          .run(IO.raiseError(new Exception("oops")))
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
    var sStart = 0
    var sStop  = 0
    var aStart = 0
    var aFail  = 0
    var others = 0

    val res = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .updateConfig(_.withQueueCapacity(2))
      .eventStream { gd =>
        gd.notice
          .updateConfig(_.withFibonacciBackoff(0.1.second, 3))
          .run(IO.raiseError(new ControlThrowable("fatal error") {}))
      }
      .evalTap {
        case _: ServiceStart => IO(sStart += 1)
        case _: ServiceStop  => IO(sStop += 1)
        case _: ActionStart  => IO(aStart += 1)
        case _: ActionFail   => IO(aFail += 1)
        case _               => IO(others += 1)
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .evalTap(console.simple[IO])
      .compile
      .toList

    assertThrows[Throwable](res.unsafeRunSync())
    assert(sStart == 1)
    assert(aStart == 1)
    assert(aFail == 1)
    assert(sStop == 1)
    assert(others == 0)
  }

  test("4.json codec") {
    val a :: b :: c :: d :: e :: f :: g :: _ = guard
      .updateConfig(_.withJitterBackoff(30.minutes, 1.hour))
      .updateConfig(_.withQueueCapacity(3))
      .eventStream { gd =>
        gd.span("json-codec")
          .notice
          .updateConfig(_.withConstantDelay(0.1.second, 3))
          .run(IO.raiseError(new Exception("oops")))
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
      .eventStream(_.retry(IO.never).run)
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
      .eventStream(gd => gd.span("a").notice.retry(IO(1)).run >> gd.span("b").notice.retry(IO(2)).run)
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
      gd.span("s1-a1").notice.retry(IO(1)).run >> gd.span("s1-a2").notice.retry(IO(2)).run)
    val ss2 = s2.eventStream(gd =>
      gd.span("s2-a1").notice.retry(IO(1)).run >> gd.span("s2-a2").notice.retry(IO(2)).run)

    val vector = ss1.merge(ss2).compile.toVector.unsafeRunSync()
    assert(vector.count(_.isInstanceOf[ActionSucc]) == 4)
    assert(vector.count(_.isInstanceOf[ServiceStop]) == 2)
  }

  test("9.print agent params") {
    guard.eventStream(ag => IO.println(ag.zoneId)).compile.drain.unsafeRunSync()
  }

  test("10.span") {
    guard.eventStream { ag =>
      val s1 = ag.span("a").span("b").span("c").agentParams.spans
      val s2 = ag.span(List(Name("a"), Name("b"), Name("c"))).agentParams.spans
      IO(assert(s1 === s2))
    }.debug().compile.drain.unsafeRunSync()
  }

  test("11.should give up") {
    var serviceStart    = 0
    var actionStart     = 0
    var actionRetry     = 0
    var actionFail      = 0
    var serviceStop     = 0
    var shouldNotHappen = 0
    val action = guard
      .updateConfig(_.withAlwaysGiveUp)
      .eventStream { gd =>
        gd.span("give-up")
          .notice
          .updateConfig(_.withFibonacciBackoff(0.1.second, 3))
          .run(IO.raiseError(new Exception))
      }
      .evalTap { case event: ServiceEvent =>
        event match {
          case _: ServiceStart => IO(serviceStart += 1)
          case _: ServicePanic => IO(shouldNotHappen += 1)
          case _: ServiceStop  => IO(serviceStop += 1)
          case _: MetricEvent  => IO(shouldNotHappen += 1)
          case _: ActionStart  => IO(actionStart += 1)
          case _: ActionRetry  => IO(actionRetry += 1)
          case _: ActionFail   => IO(actionFail += 1)
          case _: ActionSucc   => IO(shouldNotHappen += 1)
          case _: InstantEvent => IO(shouldNotHappen += 1)
        }
      }
      .evalMap(e => IO(decode[NJEvent](e.asJson.noSpaces)).rethrow)
      .compile
      .drain

    assertThrows[Exception](action.unsafeRunSync())
    assert(serviceStart == 1)
    assert(actionStart == 1)
    assert(actionRetry == 3)
    assert(actionFail == 1)
    assert(serviceStop == 1)
    assert(shouldNotHappen == 0)
  }

  test("12.dummy agent should not block") {
    val dummy = TaskGuard.dummyAgent[IO].unsafeRunSync()
    dummy.span("dummy").critical.retry(IO(1)).run.replicateA(10).unsafeRunSync()
  }
}
