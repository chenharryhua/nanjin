package mtest.caffeine
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.MetricReport
import com.github.chenharryhua.nanjin.guard.instrument.caffeine.instrumentCaffeine
import com.github.chenharryhua.nanjin.guard.observers.console
import monocle.macros.GenPrism
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class CaffeineTest extends AnyFunSuite {
  private val cache: Cache[Int, String] = Caffeine
    .newBuilder()
    .maximumSize(100)
    .expireAfterWrite(Duration.ofMinutes(5))
    .recordStats()
    .build[Int, String]()

  test("caffeine") {
    val mr = TaskGuard[IO]("caffeine")
      .service("caffeine")
      .eventStream { ga =>
        instrumentCaffeine(ga, cache, "caffeine").surround(
          ga.action("put").delay(cache.put(1, "a")).buildWith(identity).use(_.run(()) >> ga.metrics.report))
      }
      .evalTap(console.text[IO])
      .mapFilter(GenPrism[NJEvent, MetricReport].getOption)
      .compile
      .lastOrError
      .unsafeRunSync()
    assert(mr.snapshot.gauges.last.value.hcursor.downField("hitRate").as[Long] == Right(1))

  }
}
