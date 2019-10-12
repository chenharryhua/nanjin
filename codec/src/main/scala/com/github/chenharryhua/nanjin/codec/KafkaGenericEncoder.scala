package com.github.chenharryhua.nanjin.codec

import fs2.Chunk

sealed abstract class KafkaGenericEncoder[K, V] {
  def single[F[_, _]](k: K, v: V): F[K, V]
  def multi[F[_, _]](kvs: Chunk[(K, V)]): F[K, V]
}
