package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.stats.CacheStats
import com.github.chenharryhua.nanjin.guard.translator.fmt
import io.circe.{Encoder, Json}

import scala.concurrent.duration.DurationLong

trait CaffeineCache[F[_], K, V] {
  def getIfPresent(key: K): F[Option[V]]
  def get(key: K, fv: => F[V]): F[V]

  def put(key: K, value: V): F[Unit]

  def updateWith(key: K)(f: Option[V] => V): F[V]

  def invalidate(key: K): F[Unit]

  def getStats: F[CaffeineCache.Stats]
}

object CaffeineCache {
  final case class Stats(
    hitCount: Long,
    missCount: Long,
    loadSuccessCount: Long,
    loadFailureCount: Long,
    totalLoadTime: String,
    evictionCount: Long,
    evictionWeight: Long)

  implicit val encoderStats: Encoder[Stats] = (a: Stats) =>
    Json.fromString(s"[hit: ${a.hitCount}, miss: ${a.missCount}, evict: ${a.evictionCount}]")

  private object Stats {

    def apply(cs: CacheStats): Stats = Stats(
      hitCount = cs.hitCount(),
      missCount = cs.missCount(),
      loadSuccessCount = cs.loadSuccessCount(),
      loadFailureCount = cs.loadFailureCount(),
      totalLoadTime = fmt.format(cs.totalLoadTime().nano),
      evictionCount = cs.evictionCount(),
      evictionWeight = cs.evictionWeight()
    )
  }

  final private class Impl[F[_], K, V](cache: Cache[K, V])(implicit F: Sync[F])
      extends CaffeineCache[F, K, V] {

    override def getIfPresent(key: K): F[Option[V]] =
      F.delay(Option(cache.getIfPresent(key)))

    override def get(key: K, fv: => F[V]): F[V] =
      getIfPresent(key).flatMap {
        case Some(value) => F.pure(value)
        case None        => F.defer(fv).map(v => cache.get(key, (_: K) => v))
      }

    override def put(key: K, value: V): F[Unit] =
      F.delay(cache.put(key, value))

    override def updateWith(key: K)(f: Option[V] => V): F[V] =
      getIfPresent(key).map(f).flatTap(v => put(key, v))

    override def invalidate(key: K): F[Unit] =
      F.delay(cache.invalidate(key))

    override val getStats: F[Stats] =
      F.delay(Stats(cache.stats()))

    val invalidateAll: F[Unit] =
      F.delay(cache.invalidateAll())

  }

  private[guard] def buildCache[F[_], K, V](cache: Cache[K, V])(implicit
    F: Sync[F]): Resource[F, CaffeineCache[F, K, V]] =
    Resource.make(F.pure(new Impl(cache)))(_.invalidateAll)
}
