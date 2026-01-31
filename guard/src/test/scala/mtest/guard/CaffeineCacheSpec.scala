package mtest.guard

import cats.effect.kernel.Async
import cats.effect.unsafe.IORuntime
import cats.effect.{Deferred, IO, Resource}
import com.github.benmanes.caffeine.cache.Caffeine
import com.github.chenharryhua.nanjin.guard.action.CaffeineCache
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class CaffeineCacheSpec extends AsyncFlatSpec with Matchers {

  implicit val runtime: IORuntime = IORuntime.global

  def cacheResource[F[_]: Async, K, V]: Resource[F, CaffeineCache[F, K, V]] =
    CaffeineCache[F, K, V](Caffeine.newBuilder().maximumSize(1000).build[K, V]())

  "CaffeineCache" should "put and getIfPresent values" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        _ <- cache.put("a", 10)
        res <- cache.getIfPresent("a")
      } yield res shouldBe Some(10)
    }.unsafeToFuture()

  it should "get returns cached value without recomputation" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        _ <- cache.put("a", 42)
        res <- cache.get("a", IO.pure(99))
      } yield res shouldBe 42
    }.unsafeToFuture()

  it should "get computes value if missing" in
    cacheResource[IO, String, Int].use { cache =>
      cache.get("a", IO.pure(99)).map(_ shouldBe 99)
    }.unsafeToFuture()

  it should "updateWith modifies existing value" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        _ <- cache.put("a", 10)
        res <- cache.updateWith("a")(v => IO.pure(v.getOrElse(0) + 5))
        after <- cache.getIfPresent("a")
      } yield {
        res shouldBe 15
        after shouldBe Some(15)
      }
    }.unsafeToFuture()

  it should "updateWith creates value if missing" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        res <- cache.updateWith("a")(v => IO.pure(v.getOrElse(0) + 7))
        after <- cache.getIfPresent("a")
      } yield {
        res shouldBe 7
        after shouldBe Some(7)
      }
    }.unsafeToFuture()

  it should "invalidate removes a key" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        _ <- cache.put("a", 1)
        _ <- cache.invalidate("a")
        res <- cache.getIfPresent("a")
      } yield res shouldBe None
    }.unsafeToFuture()

  it should "invalidateAll clears all keys" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        _ <- cache.putAll(Map("a" -> 1, "b" -> 2))
        _ <- cache.invalidateAll
        resA <- cache.getIfPresent("a")
        resB <- cache.getIfPresent("b")
      } yield {
        resA shouldBe None
        resB shouldBe None
      }
    }.unsafeToFuture()

  it should "get is single-flight" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        gate <- Deferred[IO, Unit]
        fiber1 <- cache.get("a", gate.get *> IO.pure(10)).start
        _ <- IO.sleep(100.millis)
        fiber2 <- cache.get("a", IO.pure(99)).start
        _ <- IO.sleep(50.millis)
        _ <- gate.complete(())
        r1 <- fiber1.joinWithNever
        r2 <- fiber2.joinWithNever
      } yield {
        r1 shouldBe 10
        r2 shouldBe 10
      }
    }.unsafeToFuture()

  it should "updateWith is single-flight" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        gate <- Deferred[IO, Unit]
        fiber1 <- cache.updateWith("a")(_ => gate.get *> IO.pure(10)).start
        fiber2 <- cache.updateWith("a")(_ => IO.pure(99)).start
        _ <- IO.sleep(50.millis)
        _ <- gate.complete(())
        r1 <- fiber1.joinWithNever
        r2 <- fiber2.joinWithNever
      } yield {
        r1 shouldBe 10
        r2 shouldBe 10
      }
    }.unsafeToFuture()

  it should "getStats reflects cache size and in-flight" in
    cacheResource[IO, String, Int].use { cache =>
      for {
        _ <- cache.put("a", 1)
        _ <- cache.get("a", IO.pure(0))
        stats <- cache.getStats
      } yield {
        stats.cached should be >= 1L
        stats.inFlights should be >= 0
      }
    }.unsafeToFuture()

}
