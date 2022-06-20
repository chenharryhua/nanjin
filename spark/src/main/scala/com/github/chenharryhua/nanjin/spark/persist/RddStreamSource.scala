package com.github.chenharryhua.nanjin.spark.persist

import akka.stream.scaladsl.Source
import akka.NotUsed
import cats.effect.kernel.Sync
import com.github.chenharryhua.nanjin.common.ChunkSize
import fs2.Stream
import org.apache.spark.rdd.RDD

final class RddStreamSource[F[_], A](rdd: RDD[A]) extends Serializable {

  // fs2 stream
  def stream(chunkSize: ChunkSize)(implicit F: Sync[F]): Stream[F, A] =
    Stream.fromBlockingIterator(rdd.toLocalIterator, chunkSize.value)

  // akka source
  def source: Source[A, NotUsed] = Source.fromIterator(() => rdd.toLocalIterator)

}
