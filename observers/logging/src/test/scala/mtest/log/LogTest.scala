package mtest.log

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.logging.log
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

class LogTest extends AnyFunSuite {

  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .updateConfig(_.addBrief(Json.fromString("brief")))
      .eventStream { agent =>
        val fac = agent.facilitator("job")
        val ag = fac.metrics
        val go = for {
          _ <- ag.gauge("a").register(IO(1000000000))
          _ <- ag.healthCheck("b").register(IO(true))
          _ <- ag.timer("c").evalMap(_.update(10.second).replicateA(100))
          _ <- ag.meter("d", _.withUnit(_.COUNT)).evalMap(_.update(10000).replicateA(100))
          _ <- ag.counter("e", _.asRisk).evalMap(_.inc(1000))
          _ <- ag.histogram("f", _.withUnit(_.BYTES)).evalMap(_.update(10000L).replicateA(100))
          _ <- agent
            .action("h", _.timed.counted)
            .retry(IO(0))
            .buildWith(identity)
            .evalMap(_.run(()).replicateA(100))
          _ <- ag.ratio("i").evalMap(f => f.incDenominator(50) >> f.incNumerator(79) >> f.incBoth(20, 50))
        } yield ()
        go.surround(agent.adhoc.report)
      }

  test("1.logging json") {
    service.evalTap(log.json[IO]).compile.drain.unsafeRunSync()
  }

  test("2.logging simple") {
    service.evalTap(log.text[IO]).compile.drain.unsafeRunSync()
  }

}
