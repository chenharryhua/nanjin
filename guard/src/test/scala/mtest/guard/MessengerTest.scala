package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite
import fs2.Stream
import scala.concurrent.duration.DurationInt

class MessengerTest extends AnyFunSuite {
  val service: Stream[IO, NJEvent] = TaskGuard[IO]("messenger")
    .service("messenger")
    .eventStream(agent =>
      agent
        .facilitate("messenger and metrics")(fac =>
          for {
            counter <- fac.metrics.counter("counter")
            meter <- fac.metrics.meter("meter")
            timer <- fac.metrics.timer("timer")
            histo <- fac.metrics.histogram("histogram")
            idle <- fac.metrics.idleGauge("idle")
            _ <- fac.metrics.activeGauge("active")
            _ <- fac.metrics.gauge("gauge").register(IO(0))
            _ <- fac.metrics.healthCheck("health").register(IO(false))
          } yield Kleisli((_: Unit) =>
            fac.messenger.warn("a") *>
              fac.messenger.consoleWarn("a") *>
              fac.messenger.done("b") *>
              fac.messenger.consoleDone("b") *>
              fac.messenger.info("c") *>
              fac.messenger.consoleInfo("c") *>
              fac.messenger.error("d") *>
              fac.messenger.consoleError("d") *>
              counter.inc(1) *>
              meter.update(1) *>
              histo.update(1) *>
              timer.update(1.second) *>
              idle.run(())))
        .use(_.run(()) >> agent.adhoc.report))

  test("text") {
    service.map(checkJson).evalTap(console.text[IO]).compile.drain.unsafeRunSync()
  }
  test("json") {
    service.map(checkJson).evalTap(console.json[IO]).compile.drain.unsafeRunSync()
  }

}
