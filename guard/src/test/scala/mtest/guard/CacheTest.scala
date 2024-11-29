package mtest.guard

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.action.RemovalCache
import com.github.chenharryhua.nanjin.guard.observers.console
import org.scalatest.funsuite.AnyFunSuite
import fs2.Stream

import java.time.Duration
import scala.concurrent.duration.DurationInt

class CacheTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("cache").service("cache")

  test("1.put get") {
    service.eventStream { agent =>
      agent.caffeineCache(Caffeine.newBuilder().build[Int, Int]()).use { cache =>
        cache.put(1, 101) >> cache.getIfPresent(1).flatMap(agent.herald.done(_))
      }
    }.debug().compile.drain.unsafeRunSync()
  }

  test("2.removal") {
    service
      .updateConfig(_.withMetricReport(_.crontab(_.every3Seconds)))
      .eventStream { agent =>
        val run = for {
          RemovalCache(cache, stream) <- agent.removalCache[Int, Int](
            _.expireAfterWrite(Duration.ofSeconds(3)).recordStats())
          _ <- agent.facilitate("removal.cache")(_.gauge("cache.stats").register(cache.getStats))
        } yield Stream
          .range(1, 30)
          .covary[IO]
          .metered(1.seconds)
          .evalMap(i => cache.put(i, i).replicateA_(10) >> cache.get(i,IO(0)))
          .concurrently(stream.debug())
          .compile
          .drain
        run.use(identity)
      }
      .evalTap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

}
