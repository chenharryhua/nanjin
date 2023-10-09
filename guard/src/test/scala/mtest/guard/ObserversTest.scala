package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.*
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ObserversTest"
class ObserversTest extends AnyFunSuite {

  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .withBrief(Json.fromString("brief"))
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.seconds)))
      .eventStream { ag =>
        val box = ag.atomicBox(1)
        val job = // fail twice, then success
          box.getAndUpdate(_ + 1).map(_ % 3 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
        val meter = ag.meter("meter").counted
        val action = ag
          .action(
            "nj_error",
            _.critical.bipartite.timed.counted.policy(policies.fixedRate(1.second).limited(1)))
          .retry(job)
          .logInput(Json.fromString("input data"))
          .logOutput(_ => Json.fromString("output data"))
          .logErrorM(ex =>
            IO.delay(
              Json.obj("developer's advice" -> "no worries".asJson, "message" -> ex.getMessage.asJson)))
          .run

        val counter   = ag.counter("nj counter").asRisk
        val histogram = ag.histogram("nj histogram", _.DAYS).counted
        val alert     = ag.alert("nj alert")
        val gauge     = ag.gauge("nj gauge")

        gauge
          .register(100)
          .surround(
            gauge.timed.surround(
              action >>
                meter.mark(1000) >>
                counter.inc(10000) >>
                histogram.update(10000000000000L) >>
                alert.error("alarm") >>
                ag.metrics.report))
      }

  test("1.logging verbose") {
    service.evalTap(logging.verbose[IO]).compile.drain.unsafeRunSync()
  }

  test("1.2.logging json") {
    service.evalTap(logging.json[IO].withLoggerName("json")).compile.drain.unsafeRunSync()
  }

  test("1.3 logging simple") {
    service.evalTap(logging.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("2.console - simple text") {
    service.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("3.console - pretty json") {
    service.evalTap(console.json[IO]).compile.drain.unsafeRunSync()
  }

  test("3.1.console - simple json") {
    service.evalTap(console.simpleJson[IO]).compile.drain.unsafeRunSync()
  }

  test("3.2.console - verbose json") {
    service.evalTap(console.verboseJson[IO]).compile.drain.unsafeRunSync()
  }

}
