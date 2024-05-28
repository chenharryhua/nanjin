package com.github.chenharryhua.nanjin.guard.instrument.caffeine

import cats.Eval
import cats.effect.kernel.Resource
import com.github.benmanes.caffeine.cache.Cache
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.circe.generic.JsonCodec

import java.time.Duration

@JsonCodec
final private[caffeine] case class Statistics(
  hitCount: Long,
  hitRate: Double,
  missCount: Long,
  missRate: Double,
  loadCount: Long,
  loadSuccessCount: Long,
  loadFailureCount: Long,
  loadFailureRate: Double,
  totalLoadTime: String,
  averageLoadPenalty: Double,
  evictionCount: Long,
  evictionWeight: Long
)
object instrumentCaffeine {
  private val fmt = DurationFormatter.defaultFormatter
  def apply[F[_]](agent: Agent[F], cache: Cache[?, ?], name: String): Resource[F, Unit] =
    agent
      .gauge(name)
      .instrument(Eval.always {
        val stats = cache.stats()
        Statistics(
          hitCount = stats.hitCount(),
          hitRate = stats.hitRate(),
          missCount = stats.missCount(),
          missRate = stats.missRate(),
          loadCount = stats.loadCount(),
          loadSuccessCount = stats.loadSuccessCount(),
          loadFailureCount = stats.loadFailureCount(),
          loadFailureRate = stats.loadFailureRate(),
          totalLoadTime = fmt.format(Duration.ofNanos(stats.totalLoadTime())),
          averageLoadPenalty = stats.averageLoadPenalty(),
          evictionCount = stats.evictionCount(),
          evictionWeight = stats.evictionWeight()
        )
      })
}
