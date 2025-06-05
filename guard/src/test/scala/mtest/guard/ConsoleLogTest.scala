package mtest.guard

import cats.data.Kleisli
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toFunctorFilterOps
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.{eventFilters, Event}
import com.github.chenharryhua.nanjin.guard.observers.*
import io.circe.Json
import org.scalatest.funsuite.AnyFunSuite
import squants.Each
import squants.information.Bytes

import scala.concurrent.duration.*

// sbt "guard/testOnly mtest.guard.ConsoleLogTest"
class ConsoleLogTest extends AnyFunSuite {

  val service: fs2.Stream[IO, Event] =
    TaskGuard[IO]("nanjin")
      .service("observing")
      .updateConfig(_.addBrief(Json.fromString("brief")))
      .eventStream { agent =>
        val mtx = agent.facilitate("job") { mtx =>
          for {
            _ <- mtx.gauge("7").register(IO(1000000000))
            _ <- mtx.healthCheck("6").register(IO(true))
            _ <- mtx.timer("5").evalMap(_.elapsed(10.second).replicateA(100))
            _ <- mtx.meter(Each)("4", _.enable(true)).evalMap(_.mark(10000).replicateA(100))
            _ <- mtx.counter("3", _.asRisk).evalMap(_.inc(1000))
            _ <- mtx.histogram(Bytes)("2").evalMap(_.update(10000L).replicateA(100))
            _ <- mtx
              .percentile("1")
              .evalMap(f => f.incDenominator(500) >> f.incNumerator(60) >> f.incBoth(299, 500))
          } yield Kleisli((_: Int) => IO.unit)
        }

        mtx.use(_.run(1) >> agent.herald.error(new Exception())("error messages") >> agent.adhoc.report)
      }

  test("1.console - verbose json") {
    val mr = service
      .evalTap(console.verbose[IO])
      .map(checkJson)
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(!mr.snapshot.hasDuplication)
  }

  test("2.console - pretty json") {
    service.evalTap(console.json[IO]).map(checkJson).compile.drain.unsafeRunSync()
  }

  test("3.console - simple text") {
    val mr = service
      .evalTap(console.text[IO])
      .map(checkJson)
      .mapFilter(eventFilters.metricReport)
      .compile
      .lastOrError
      .unsafeRunSync()
    val tags = mr.snapshot.metricIDs.sortBy(_.metricName.age).map(_.metricName.name.toInt)
    assert(tags == List(7, 6, 5, 4, 3, 2, 1))
  }
}
