package com.github.chenharryhua.nanjin.guard.instrument.neo4j

import cats.Eval
import cats.effect.kernel.Resource
import com.github.chenharryhua.nanjin.guard.service.Agent
import io.circe.generic.JsonCodec
import org.neo4j.driver.Driver

import java.time.Duration
import scala.concurrent.duration.DurationLong
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.DurationConverters.ScalaDurationOps

@JsonCodec
final private[neo4j] case class Statistics(
  id: String,
  inUse: Int,
  idle: Int,
  creating: Int,
  created: Long,
  failedToCreate: Long,
  closed: Long,
  acquiring: Int,
  acquired: Long,
  timedOutToAcquire: Long,
  totalAcquisitionTime: Duration,
  totalConnectionTime: Duration,
  totalInUseTime: Duration,
  totalInUseCount: Long
)

object instrumentNeo4j {
  def apply[F[_]](agent: Agent[F], driver: Driver, name: String): Resource[F, Unit] =
    agent
      .gauge(name)
      .instrument(Eval.always {
        driver.metrics().connectionPoolMetrics().asScala.toList.map { stats =>
          Statistics(
            id = stats.id(),
            inUse = stats.inUse(),
            idle = stats.idle(),
            creating = stats.creating(),
            created = stats.created(),
            failedToCreate = stats.failedToCreate(),
            closed = stats.closed(),
            acquiring = stats.acquiring(),
            acquired = stats.acquired(),
            timedOutToAcquire = stats.timedOutToAcquire(),
            totalAcquisitionTime = stats.totalAcquisitionTime().milliseconds.toJava,
            totalConnectionTime = stats.totalConnectionTime().milliseconds.toJava,
            totalInUseTime = stats.totalInUseTime().milliseconds.toJava,
            totalInUseCount = stats.totalInUseCount()
          )
        }
      })
}
