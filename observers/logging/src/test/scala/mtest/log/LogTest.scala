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
      .eventStream { ag =>
        val go = for {
          _ <- ag.gauge("job").register(IO(1000000000))
          _ <- ag.healthCheck("job").register(IO(true))
          _ <- ag.gauge("job").timed
          _ <- ag.jvmGauge.garbageCollectors
          _ <- ag.jvmGauge.classloader
          _ <- ag.timer("job", _.counted).evalMap(_.update(10.second).replicateA(100))
          _ <- ag.meter("job", _.withUnit(_.COUNT).counted).evalMap(_.update(10000).replicateA(100))
          _ <- ag.counter("job", _.asRisk).evalMap(_.inc(1000))
          _ <- ag.histogram("job", _.withUnit(_.BYTES).counted).evalMap(_.update(10000L).replicateA(100))
          _ <- ag.alert("job", _.counted).evalMap(_.error("alarm"))
          _ <- ag.flowMeter("job", _.withUnit(_.KILOBITS).counted).evalMap(_.update(200000))
          _ <- ag
            .action("job", _.timed.counted)
            .retry(IO(0))
            .buildWith(identity)
            .evalMap(_.run(()).replicateA(100))
          _ <- ag
            .ratio("job")
            .evalMap(f => f.incDenominator(50) >> f.incNumerator(79.99) >> f.incBoth(20.0, 50))
        } yield ()
        go.surround(ag.metrics.report)
      }

  test("1.logging json") {
    service.evalTap(log.json[IO]).compile.drain.unsafeRunSync()
  }

  test("2.logging simple") {
    service.evalTap(log.text[IO]).compile.drain.unsafeRunSync()
  }

}
