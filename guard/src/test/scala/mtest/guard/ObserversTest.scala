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
      .updateConfig(_.withRestartPolicy(policies.fixedDelay(1.seconds))
        .withMetricReport(policies.crontab(_.secondly).limited(3)))
      .eventStream { ag =>
        val box = ag.atomicBox(1)
        val job = // fail twice, then success
          box.getAndUpdate(_ + 1).map(_ % 3 == 0).ifM(IO(1), IO.raiseError[Int](new Exception("oops")))
        val meter = ag.meter("meter", _.COUNT).counted
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

        val counter = ag.counter("nj counter").asRisk
        val histo1  = ag.histogram("histo1", _.MICROSECONDS).counted
        val histo2  = ag.histogram("histo2", _.BYTES_SECOND).counted
        val histo3  = ag.histogram("histo3", _.MEGABYTES).counted
        val histo4  = ag.histogram("histo4", _.COUNT)
        val alert   = ag.alert("nj alert")
        val gauge   = ag.gauge("nj gauge")

        gauge
          .register(100)
          .surround(
            gauge.timed.surround(
              action >>
                meter.mark(1000) >>
                counter.inc(10000) >>
                histo1.update(10000000000000L) >>
                histo2.update(2000) >>
                histo3.update(300) >>
                histo4.update(4) >>
                alert.error("alarm") >>
                ag.metrics.report)) >> ag.metrics.reset
      }

  test("2.logging json") {
    service.evalTap(logging.json[IO].withLoggerName("json")).compile.drain.unsafeRunSync()
  }

  test("3 logging simple") {
    service.evalTap(logging.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("4.console - simple text") {
    service.evalTap(console.simple[IO]).compile.drain.unsafeRunSync()
  }

  test("6.console - pretty json") {
    service.evalTap(console.json[IO]).compile.drain.unsafeRunSync()
  }

  test("7.console - simple json") {
    service.evalTap(console.simpleJson[IO]).compile.drain.unsafeRunSync()
  }

  test("8.console - verbose json") {
    service.evalTap(console.verboseJson[IO]).compile.drain.unsafeRunSync()
  }

}
