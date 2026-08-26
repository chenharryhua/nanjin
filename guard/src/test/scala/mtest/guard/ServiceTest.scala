package mtest.guard

import cats.effect.IO
import cats.effect.std.AtomicCell
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.common.chrono.zones.londonTime
import com.github.chenharryhua.nanjin.common.chrono.{Policy, Tick}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.event.Event.*
import com.github.chenharryhua.nanjin.guard.event.StopReason.Successfully
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

class ServiceTest extends AnyFunSuite {

  val guard: TaskGuard[IO] = TaskGuard[IO]("service-level-guard").updateConfig(
    _.withHomepage("https://abc.com/efg")
      .withZoneId(londonTime)
      .withRestartPolicy(1.hour, _.fixedDelay(1.seconds).repeat)
      .withLogThreshold(_.Debug, _.Debug)
      .withHistoryCapacity(32, 32, 32)
      .addBrief(Json.fromString("test")))

  val policy: Policy = Policy.fixedDelay(0.1.seconds).repeat.limited(3)

  test("1.should stopped if the operation normally exits") {
    val List(a, b) = guard.service("exit").eventStream(_ => IO(())).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ServiceStop].cause == Successfully)
  }

  test("2.escalate to up level if retry failed") {
    val List(a, b, c, d, e, f, g, h) = guard
      .service("retry")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(1.seconds).repeat.limited(1)))
      .eventStream { ga =>
        ga.retry(_.withPolicy(_.fixedDelay(1.seconds).repeat.limited(1)))
          .use(_(ga.logger.info("info") *> IO.raiseError(new Exception)))
      }
      .compile
      .toList
      .unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.isInstanceOf[ReportedEvent])
    assert(c.isInstanceOf[ReportedEvent])
    assert(d.isInstanceOf[ServicePanic])
    assert(e.isInstanceOf[ServiceStart])
    assert(f.isInstanceOf[ReportedEvent])
    assert(g.isInstanceOf[ReportedEvent])
    assert(h.isInstanceOf[ServiceStop])
  }

  test("3.policy start over") {

    val p1 = Policy.fixedDelay(1.seconds).repeat.limited(1)
    val p2 = Policy.fixedDelay(2.seconds).repeat.limited(1)
    val p3 = Policy.fixedDelay(3.seconds).repeat.limited(1)
    val policy = p1.followedBy(p2).followedBy(p3).repeat
    val List(a, b, c, d, e, f, g, h) = guard
      .service("start over")
      .updateConfig(_.withRestartPolicy(2.hour, _ => policy))
      .eventStream(_ => IO.raiseError[Int](new Exception("oops")).void)
      .map(checkJson)
      .evalMapFilter[IO, Tick] {
        case sp: ServicePanic => IO(Some(sp.tick))
        case _                => IO(None)
      }
      .take(8)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.index == 1)
    assert(b.index == 2)
    assert(c.index == 3)
    assert(d.index == 4)
    assert(e.index == 5)
    assert(f.index == 6)
    assert(g.index == 7)
    assert(h.index == 8)

    assert(b.commence == a.conclude)
    assert(c.commence == b.conclude)
    assert(d.commence == c.conclude)
    assert(e.commence == d.conclude)
    assert(f.commence == e.conclude)
    assert(g.commence == f.conclude)
    assert(h.commence == g.conclude)

    assert(a.snooze == 1.second.toJava)
    assert(b.snooze == 2.second.toJava)
    assert(c.snooze == 3.second.toJava)
    assert(d.snooze == 1.second.toJava)
    assert(e.snooze == 2.second.toJava)
    assert(f.snooze == 3.second.toJava)
    assert(g.snooze == 1.second.toJava)
    assert(h.snooze == 2.second.toJava)
  }

  test("4.policy threshold start over") {

    val policy: Policy = Policy.fixedDelay(1.seconds, 2.seconds, 3.seconds, 4.seconds, 5.seconds).repeat
    val List(a, b, c) =
      fs2.Stream
        .eval(AtomicCell[IO].of(0.seconds))
        .flatMap { box =>
          guard
            .service("threshold")
            .updateConfig(_.withRestartPolicy(2.seconds, _ => policy))
            .eventStream { _ =>
              box.getAndUpdate(_ + 1.second).flatMap(IO.sleep) <*
                IO.raiseError[Int](new Exception("oops"))
            }
            .map(checkJson)
            .evalMapFilter[IO, Tick] {
              case sp: ServicePanic => IO(Some(sp.tick))
              case _                => IO(None)
            }
        }
        .take(3)
        .compile
        .toList
        .unsafeRunSync()

    assert(a.index == 1)
    assert(b.index == 2)
    assert(c.index == 3)

    assert(b.commence == a.conclude)
    assert(c.commence == b.conclude)

    assert(a.snooze == 1.second.toJava)
    assert(b.snooze == 2.second.toJava)
    assert(c.snooze == 1.second.toJava)
  }

  test("5.service config") {
    TaskGuard[IO]("abc")
      .service("abc")
      .updateConfig(_.withRestartPolicy(2.seconds, _.fixedDelay(1.second).repeat)
        .withMetricsReport(_.crontab(_.secondly).repeat))
      .eventStreamR(_.facilitate("nothing")(_.counter("counter")))
      .map(checkJson)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("6.closure - io") {
    val List(a, b) = guard
      .service("closure")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(1.seconds).repeat.limited(1)))
      .eventStream { agent =>
        val a = UUID.randomUUID()
        agent.logger.warn(a.toString) *> IO.raiseError(new Exception)
      }
      .mapFilter(Event.reportedEvent.getOption)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.message.value.as[String].toOption.get != b.message.value.as[String].toOption.get)
  }

  test("7.closure - stream") {
    val List(a, b) = guard
      .service("closure")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(1.seconds).repeat.limited(1)))
      .eventStreamS { agent =>
        val a = UUID.randomUUID()

        fs2.Stream(0).covary[IO].evalMap(_ => agent.logger.info(a.toString) *> IO.raiseError(new Exception))

      }
      .mapFilter(Event.reportedEvent.getOption)
      .compile
      .toList
      .unsafeRunSync()
    assert(a.message.value.as[String].toOption.get != b.message.value.as[String].toOption.get)
  }

  test("8.exception thrown elsewhere") {
    val res = guard
      .service("ex")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedRate(1.seconds).repeat.limited(1)))
      .eventStream { _ =>
        Future[Int] {
          Thread.sleep(2_000)
          throw new Exception("oops")
        }(using scala.concurrent.ExecutionContext.Implicits.global)
        IO.sleep(5.seconds)
      }
      .compile
      .toList
      .unsafeRunSync()
    assert(res.head.isInstanceOf[ServiceStart])
    assert(res(1).asInstanceOf[ServiceStop].cause == StopReason.Successfully)
  }

  test("9.by cancellation - internal") {
    val List(a, b) =
      guard.service("cancel").eventStream(_ => IO.unit <* IO.canceled).compile.toList.unsafeRunSync()
    assert(a.isInstanceOf[ServiceStart])
    assert(b.asInstanceOf[ServiceStop].cause == StopReason.ByCancellation)
  }

  test("10.by cancellation - external") {
    val res: List[Event] =
      guard
        .service("cancel")
        .eventStream(_.logger.error("oops").delayBy(1.seconds).replicateA_(1000))
        .take(5)
        .compile
        .toList
        .unsafeRunSync()
    assert(res.last.isInstanceOf[ReportedEvent])
  }

  test("11.watchdog retries a failing service according to restart policy") {
    val events = guard
      .service("watchdog")
      .updateConfig(_.withRestartPolicy(1.hour, _.fixedDelay(100.millis).repeat.limited(2)))
      .eventStream(_ => IO.raiseError(new Exception("boom")).void)
      .map(checkJson)
      .compile
      .toList
      .unsafeRunSync()

    assert(events.count(_.isInstanceOf[ServiceStart]) == 3)
    assert(events.count(_.isInstanceOf[ServicePanic]) == 2)
    assert(events.last.asInstanceOf[ServiceStop].cause.exitCode == 3)
  }

}
