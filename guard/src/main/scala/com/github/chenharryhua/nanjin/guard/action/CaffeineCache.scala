package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxApplicativeId, toFlatMapOps, toFunctorOps}
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.stats.CacheStats
import io.circe.generic.JsonCodec

trait CaffeineCache[F[_], K, V] {
  def getIfPresent(key: K): F[Option[V]]
  def get(key: K, fv: => F[V]): F[V]

  def put(key: K, value: V): F[Unit]

  def updateWith(key: K)(f: Option[V] => V): F[V]

  def invalidate(key: K): F[Unit]
  def invalidateAll: F[Unit]

  def getStats: F[CaffeineCache.Stats]
}

object CaffeineCache {
  @JsonCodec
  final case class Stats(hits: Long, missed: Long, cached: Long)

  private object Stats {

    def apply(cs: CacheStats, estimatedSize: Long): Stats = Stats(
      hits = cs.hitCount(),
      missed = cs.missCount(),
      cached = estimatedSize
    )
  }

  final private class Impl[F[_], K, V](cache: Cache[K, V])(implicit F: Sync[F])
      extends CaffeineCache[F, K, V] {

    override def getIfPresent(key: K): F[Option[V]] =
      F.delay(Option(cache.getIfPresent(key)))

    override def put(key: K, value: V): F[Unit] =
      F.delay(cache.put(key, value))

    override def get(key: K, fv: => F[V]): F[V] =
      getIfPresent(key).flatMap {
        case Some(value) => F.pure(value)
        case None        => F.defer(fv).flatTap(v => put(key, v))
      }

    override def updateWith(key: K)(f: Option[V] => V): F[V] =
      getIfPresent(key).map(f).flatTap(v => put(key, v))

    override def invalidate(key: K): F[Unit] =
      F.delay(cache.invalidate(key))

    override val invalidateAll: F[Unit] =
      F.delay(cache.invalidateAll())

    override val getStats: F[Stats] =
      F.delay(Stats(cache.stats(), cache.estimatedSize()))

    val cleanUp: F[Unit] = F.delay(cache.cleanUp())
  }

  private[guard] def build[F[_]: Sync, K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]] =
    Resource.make(new Impl(cache).pure[F])(_.cleanUp)
}
