package com.github.chenharryhua.nanjin.spark.dstream

import cats.data.Reader
import cats.effect.{Blocker, Sync}
import org.apache.spark.streaming.StreamingContext

final class EndMark private {}

private[dstream] object EndMark {
  val mark: EndMark = new EndMark
}

final class DStreamRunner[F[_]] private (sc: StreamingContext) extends Serializable {

  def signup[A](rd: Reader[StreamingContext, A])(f: A => EndMark): this.type = {
    f(rd.run(sc))
    this
  }

  def run(blocker: Blocker)(implicit F: Sync[F]): F[Unit] =
    F.delay {
      sc.start()
      sc.awaitTermination()
    }
}

object DStreamRunner {
  def apply[F[_]](sc: StreamingContext) = new DStreamRunner[F](sc)
}
