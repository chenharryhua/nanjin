package com.github.chenharryhua.nanjin.kafka

import cats.effect.Temporal
import cats.implicits.toFunctorOps
import fs2.Pipe
import fs2.kafka.{CommittableOffset, CommittableOffsetBatch}

import scala.concurrent.duration.FiniteDuration

/* Best Fs2 Kafka Lib [[https://fd4s.github.io/fs2-kafka/]]
 *
  * [[https://redpanda.com/guides/kafka-performance/kafka-performance-tuning]]
 */

package object connector {

  def commitBatch[F[_]: Temporal](n: Int, d: FiniteDuration): Pipe[F, CommittableOffset[F], Int] =
    _.groupWithin(n, d).evalMap(os => CommittableOffsetBatch.fromFoldable(os).commit.as(os.size))

}
