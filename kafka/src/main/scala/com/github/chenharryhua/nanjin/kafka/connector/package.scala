package com.github.chenharryhua.nanjin.kafka

import cats.effect.Temporal
import cats.implicits.toFunctorOps
import fs2.Pipe
import fs2.kafka.{CommittableOffset, CommittableOffsetBatch}

import scala.concurrent.duration.FiniteDuration

package object connector {

  def commitBatch[F[_]: Temporal](n: Int, d: FiniteDuration): Pipe[F, CommittableOffset[F], Int] =
    _.groupWithin(n, d).evalMap(os => CommittableOffsetBatch.fromFoldable(os).commit.as(os.size))
}
