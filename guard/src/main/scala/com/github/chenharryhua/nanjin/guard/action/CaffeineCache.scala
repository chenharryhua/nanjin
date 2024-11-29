package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.{Async, Resource, Sync}
import cats.effect.std.{Dispatcher, Queue}
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.github.benmanes.caffeine.cache.{Cache, Caffeine, RemovalCause, RemovalListener}
import com.github.benmanes.caffeine.cache.stats.CacheStats
import com.github.chenharryhua.nanjin.guard.translator.fmt
import io.circe.generic.JsonCodec
import fs2.Stream
import scala.concurrent.duration.DurationLong

trait CaffeineCache[F[_], K, V] {
  def getIfPresent(key: K): F[Option[V]]
  def get(key: K, fv: => F[V]): F[V]

  def put(key: K, value: V): F[Unit]

  /** combine the value with the cached value by the function f and update cache using the new value
    * @return
    *   the new value
    */
  def updateCombine(key: K, value: V)(f: (V, V) => V): F[V]

  def invalidate(key: K): F[Unit]
  def invalidateAll: F[Unit]

  def getStats: F[CaffeineCache.Stats]
}

final case class RemovalCache[F[_], K, V](cache: CaffeineCache[F, K, V], stream: Stream[F, V])

object CaffeineCache {
  @JsonCodec
  final case class Stats(
    hitCount: Long,
    missCount: Long,
    loadSuccessCount: Long,
    loadFailureCount: Long,
    totalLoadTime: String,
    evictionCount: Long,
    evictionWeight: Long)

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

    override def updateCombine(key: K, value: V)(f: (V, V) => V): F[V] =
      getIfPresent(key).flatMap {
        case Some(present) =>
          val nv = f(value, present)
          put(key, nv).as(nv)
        case None =>
          put(key, value).as(value)
      }

    override def invalidate(key: K): F[Unit] =
      F.delay(cache.invalidate(key))

    override val invalidateAll: F[Unit] =
      F.delay(cache.invalidateAll())

    override val getStats: F[Stats] =
      F.delay(Stats(cache.stats()))

    val cleanUp: F[Unit] =
      F.delay(cache.cleanUp())
  }

  private[guard] def buildCache[F[_], K, V](cache: Cache[K, V])(implicit
    F: Sync[F]): Resource[F, CaffeineCache[F, K, V]] =
    Resource.make(F.pure(new Impl(cache)))(_.cleanUp)

  private[guard] def buildRemovalCache[F[_], K, V](caffeine: Caffeine[K, V])(implicit
    F: Async[F]): Resource[F, RemovalCache[F, K, V]] = {
    def removeListener(dispatcher: Dispatcher[F], queue: Queue[F, V]): RemovalListener[K, V] =
      (_: K, value: V, _: RemovalCause) => dispatcher.unsafeRunSync(queue.offer(value))

    val pair = for {
      queue <- Resource.eval(Queue.unbounded[F, V])
      dispatcher <- Dispatcher.sequential[F]
      cache <- buildCache(caffeine.removalListener(removeListener(dispatcher, queue)).build[K, V]())
    } yield (cache, queue)

    pair.map { case (cache, queue) => RemovalCache(cache, Stream.fromQueueUnterminated(queue)) }
  }
}
