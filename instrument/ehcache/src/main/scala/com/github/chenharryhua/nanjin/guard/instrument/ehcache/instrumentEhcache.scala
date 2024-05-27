package com.github.chenharryhua.nanjin.guard.instrument.ehcache

import cats.Eval
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.circe.generic.JsonCodec
import io.scalaland.chimney.dsl.*
import org.ehcache.core.spi.service.StatisticsService
import org.ehcache.core.statistics.CacheStatistics

@JsonCodec
final private[ehcache] case class Statistics(
  cacheHits: Long,
  cacheHitPercentage: Float,
  cacheMisses: Long,
  cacheMissPercentage: Float,
  cacheGets: Long,
  cachePuts: Long,
  cacheRemovals: Long,
  cacheEvictions: Long,
  cacheExpirations: Long
)

object instrumentEhcache {
  def apply[F[_]](agent: Agent[F], ss: StatisticsService, name: String): Resource[F, Unit] = {
    val stats: CacheStatistics = ss.getCacheStatistics(name)
    agent.gauge(name).instrument(Eval.always(stats.into[Statistics].enableBeanGetters.transform))
  }
}
