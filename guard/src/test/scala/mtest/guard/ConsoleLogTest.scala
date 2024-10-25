package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.observers.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ConsoleLogTest"
class ConsoleLogTest extends AnyFunSuite {

  val service: fs2.Stream[IO, NJEvent] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .updateConfig(_.addBrief(Json.fromString("brief")))
      .eventStream { agent =>
        val ag = agent.metrics("job")
        val go = for {
          _ <- ag.gauge("job").register(IO(1000000000))
          _ <- ag.healthCheck("job").register(IO(true))
          _ <- ag.timer("job").evalMap(_.update(10.second).replicateA(100))
          _ <- ag.meter("job", _.withUnit(_.COUNT)).evalMap(_.update(10000).replicateA(100))
          _ <- ag.counter("job", _.asRisk).evalMap(_.inc(1000))
          _ <- ag.histogram("job", _.withUnit(_.BYTES)).evalMap(_.update(10000L).replicateA(100))
          _ <- agent.alert("job", _.counted).evalMap(_.error("alarm"))
          _ <- agent
            .action("job", _.timed.counted.bipartite)
            .retry(IO(0))
            .buildWith(identity)
            .evalMap(_.run(()))
          _ <- ag
            .ratio("job")
            .evalMap(f => f.incDenominator(500) >> f.incNumerator(60) >> f.incBoth(299, 500))
        } yield ()
        go.surround(agent.adhoc.report)
      }

  test("1.console - verbose json") {
    service.evalTap(console.verbose[IO]).map(checkJson).compile.drain.unsafeRunSync()
  }

  test("2.console - pretty json") {
    service.evalTap(console.json[IO]).map(checkJson).compile.drain.unsafeRunSync()
  }

  test("3.console - simple text") {
    service.evalTap(console.text[IO]).map(checkJson).compile.drain.unsafeRunSync()
  }
}
