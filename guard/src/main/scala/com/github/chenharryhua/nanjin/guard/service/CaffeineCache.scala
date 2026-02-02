package com.github.chenharryhua.nanjin.guard.service

import cats.effect.implicits.monadCancelOps_
import cats.effect.kernel.{Async, Deferred, Ref, Resource}
import cats.implicits.{
  catsSyntaxApplicativeError,
  catsSyntaxApplyOps,
  catsSyntaxMonadErrorRethrow,
  toFlatMapOps,
  toFoldableOps,
  toFunctorOps
}
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.stats.CacheStats
import io.circe.generic.JsonCodec

import scala.jdk.CollectionConverters.MapHasAsJava

trait CaffeineCache[F[_], K, V] {
  def getIfPresent(key: K): F[Option[V]]

  def get(key: K, fv: F[V]): F[V]

  def put(key: K, value: V): F[Unit]
  def putAll(map: Map[K, V]): F[Unit]

  def updateWith(key: K)(f: Option[V] => F[V]): F[V]

  def invalidate(key: K): F[Unit]
  def invalidateAll: F[Unit]

  def getStats: F[CaffeineCache.Stats]
}

object CaffeineCache {
  @JsonCodec
  final case class Stats(hits: Long, misses: Long, cached: Long, inFlights: Int)

  private object Stats {

    def apply(cs: CacheStats, estimatedSize: Long, inFlights: Int): Stats = Stats(
      hits = cs.hitCount(),
      misses = cs.missCount(),
      cached = estimatedSize,
      inFlights = inFlights
    )
  }

  final private class Impl[F[_], K, V](
    cache: Cache[K, V],
    inFlight: Ref[F, Map[K, Deferred[F, Either[Throwable, V]]]])(implicit F: Async[F])
      extends CaffeineCache[F, K, V] {

    override def getIfPresent(key: K): F[Option[V]] =
      F.blocking(Option(cache.getIfPresent(key)))

    override def put(key: K, value: V): F[Unit] =
      F.blocking(cache.put(key, value))

    override def putAll(map: Map[K, V]): F[Unit] =
      F.blocking(cache.putAll(map.asJava))

    override def updateWith(key: K)(f: Option[V] => F[V]): F[V] =
      inFlight.modify { m =>
        m.get(key) match {
          case Some(waiting) => m -> Left(waiting) // already updating
          case None          => m -> Right(()) // we will update
        }
      }.flatMap {
        case Left(waiting) => waiting.get.rethrow
        case Right(_)      =>
          Deferred[F, Either[Throwable, V]].flatMap { d =>
            inFlight.update(_ + (key -> d)) *>
              getIfPresent(key)
                .flatMap(f)
                .attempt
                .flatTap(_.traverse_(put(key, _)))
                .flatTap(d.complete)
                .guarantee(inFlight.update(_ - key))
                .rethrow
          }
      }

    override def get(key: K, fv: F[V]): F[V] =
      updateWith(key) {
        case Some(value) => F.pure(value)
        case None        => fv
      }

    override val getStats: F[Stats] =
      inFlight.get.map(in => Stats(cache.stats(), cache.estimatedSize(), in.size))

    override def invalidate(key: K): F[Unit] =
      F.blocking(cache.invalidate(key))

    override val invalidateAll: F[Unit] =
      F.blocking(cache.invalidateAll())

    val cleanUp: F[Unit] = F.blocking {
      cache.invalidateAll()
      cache.cleanUp()
    }
  }

  def apply[F[_], K, V](cache: Cache[K, V])(implicit F: Async[F]): Resource[F, CaffeineCache[F, K, V]] =
    Resource.make(Ref.of[F, Map[K, Deferred[F, Either[Throwable, V]]]](Map.empty).map(new Impl(cache, _)))(
      _.cleanUp)
}
