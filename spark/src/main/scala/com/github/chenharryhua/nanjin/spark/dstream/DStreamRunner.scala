package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.effect.{Blocker, Sync}
import org.apache.spark.streaming.StreamingContext

final class DStreamRunner private (sc: StreamingContext, streamings: List[Reader[Blocker, Unit]]) extends Serializable {

  def register[A](rd: Reader[StreamingContext, A])(f: A => Reader[Blocker, Unit]) =
    new DStreamRunner(sc, f(rd.run(sc)) :: streamings)

  def run[F[_]: Sync](blocker: Blocker): F[Unit] =
    Sync[F].delay {
      streamings.foreach(_(blocker))
      sc.start()
      sc.awaitTermination()
    }
}

object DStreamRunner {
  def apply(sc: StreamingContext) = new DStreamRunner(sc, Nil)
}
