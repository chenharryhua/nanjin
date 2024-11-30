package mtest.guard

import cats.effect.std.{Dispatcher, Queue}
import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}
import cats.implicits.{catsSyntaxOptionId, toFunctorFilterOps}
import com.github.benmanes.caffeine.cache.{Caffeine, RemovalCause, RemovalListener}
import com.github.chenharryhua.nanjin.guard.TaskGuard
import com.github.chenharryhua.nanjin.guard.event.ServiceStopCause.Successfully
import com.github.chenharryhua.nanjin.guard.event.eventFilters
import com.github.chenharryhua.nanjin.guard.observers.console
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.*
import scala.jdk.DurationConverters.ScalaDurationOps

class CacheTest extends AnyFunSuite {
  private val service = TaskGuard[IO]("cache").service("cache")

  test("1.put get") {
    val ss = service.eventStream { agent =>
      agent.caffeineCache(Caffeine.newBuilder().build[Int, Int]()).use { cache =>
        cache.put(1, 101) >> cache.getIfPresent(1).flatMap(agent.herald.done(_))
      }
    }.mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(ss.cause == Successfully)
  }

  test("2.invalidate") {
    val ss = service.eventStream { agent =>
      agent
        .caffeineCache(Caffeine.newBuilder().build[Int, Int])
        .use { cache =>
          cache.put(1, 1) >> cache.invalidate(1) >> cache.getIfPresent(1).map(_.ensuring(_.isEmpty))
        }
        .void
    }.mapFilter(eventFilters.serviceStop).compile.lastOrError.unsafeRunSync()
    assert(ss.cause == Successfully)
  }

  test("2.stats") {
    service
      .updateConfig(_.withMetricReport(_.crontab(_.every3Seconds)))
      .eventStream { agent =>
        val cache = for {
          cache <- agent.caffeineCache(
            Caffeine
              .newBuilder()
              .maximumSize(10)
              .expireAfterAccess(3.seconds.toJava)
              .removalListener(new RemovalListener[Int, Int] {
                override def onRemoval(key: Int, value: Int, cause: RemovalCause): Unit =
                  println((key, value, cause))
              })
              .recordStats()
              .build[Int, Int]())
          _ <- agent.facilitate("cache")(_.gauge("stats").register(cache.getStats))
        } yield cache
        cache.use { c =>
          Stream
            .range(0, 20)
            .covary[IO]
            .metered(1.second)
            .evalMap(i =>
              IO.println(i) >>
                c.get(i, IO.sleep(1000.milli) *> IO(i)) >>
                c.updateWith(i)(_.fold(0)(_ + 100)) >>
                c.getIfPresent(i).map(_.exists(_ == i + 100).ensuring(b => b)))
            .compile
            .drain >> agent.adhoc.report
        }
      }
      .evalMap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

  test("3.evict stream") {
    service
      .updateConfig(_.withMetricReport(_.crontab(_.every3Seconds)))
      .eventStream { agent =>
        val pair = for {
          dispatcher <- Dispatcher.sequential[IO]
          queue <- Resource.eval(Queue.unbounded[IO, Option[Int]])
          cache <- agent.caffeineCache(
            Caffeine
              .newBuilder()
              .maximumSize(2)
              .expireAfterAccess(3.seconds.toJava)
              .removalListener(new RemovalListener[Int, Int] {
                override def onRemoval(key: Int, value: Int, cause: RemovalCause): Unit =
                  dispatcher.unsafeRunSync(cause match {
                    case RemovalCause.EXPLICIT =>
                      IO.println(s"EXPLICIT: $key, $value") >> queue.offer(value.some)
                    case RemovalCause.REPLACED =>
                      IO.println(s"REPLACED: $key, $value")
                    case RemovalCause.COLLECTED =>
                      IO.println(s"COLLECTED: $key, $value") >> queue.offer(value.some)
                    case RemovalCause.EXPIRED =>
                      IO.println(s"EXPIRED: $key, $value") >> queue.offer(value.some)
                    case RemovalCause.SIZE =>
                      IO.println(s"SIZE: $key, $value") >> queue.offer(value.some)
                  })
              })
              .recordStats()
              .build[Int, Int]())
          _ <- agent.facilitate("cache.stream")(_.gauge("stats").register(cache.getStats))
        } yield (cache, queue)

        pair.use { case (c, queue) =>
          Stream
            .fromQueueNoneTerminated(queue)
            .concurrently(
              Stream
                .range(0, 20)
                .covary[IO]
                .metered(1.second)
                .evalMap(i =>
                  c.updateWith(i)(_ => i) >>
                    c.updateWith(i)(_.fold(0)(_ + 100)) >>
                    c.getIfPresent(i).map(_.exists(_ == i + 100).ensuring(b => b)))
                .onFinalize(c.invalidateAll >> IO.sleep(1.second) >> queue.offer(None))
            )
            .map(i => s"out: $i")
            .debug()
            .compile
            .drain >> agent.adhoc.report
        }
      }
      .evalMap(console.text[IO])
      .compile
      .drain
      .unsafeRunSync()
  }

}
