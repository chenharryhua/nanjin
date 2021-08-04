package com.github.chenharryhua.nanjin.spark.kafka

import cats.data.Chain
import cats.mtl.Tell
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.kafka.KeyValueCodecPair
import com.github.chenharryhua.nanjin.messages.kafka.*

import scala.util.{Failure, Success}

final private[kafka] class NJDecoder[F[_], K, V](codec: KeyValueCodecPair[K, V]) extends Serializable {

  def decode[G[_, _]](gaa: G[Array[Byte], Array[Byte]])(implicit
    cm: NJConsumerMessage[G],
    tell: Tell[F, Chain[Throwable]]): F[NJConsumerRecord[K, V]] = {
    val cr = cm.lens.get(gaa)
    val k  = Option(cr.key).traverse(codec.keyCodec.tryDecode)
    val v  = Option(cr.value).traverse(codec.valCodec.tryDecode)
    val nj = NJConsumerRecord(cr.bimap(_ => k.toOption.flatten, _ => v.toOption.flatten))

    val log = (k, v) match {
      case (Success(_), Success(_))   => Chain.empty
      case (Failure(ex), Success(_))  => Chain.one(ex)
      case (Success(_), Failure(ex))  => Chain.one(ex)
      case (Failure(kf), Failure(vf)) => Chain(kf, vf)
    }
    tell.writer(nj, log)
  }

  def decode(cr: NJConsumerRecord[Array[Byte], Array[Byte]])(implicit
    tell: Tell[F, Chain[Throwable]]): F[NJConsumerRecord[K, V]] = {
    val k  = cr.key.traverse(codec.keyCodec.tryDecode)
    val v  = cr.value.traverse(codec.valCodec.tryDecode)
    val nj = cr.bimap(_ => k.toOption.flatten, _ => v.toOption.flatten).flatten[K, V]

    val log = (k, v) match {
      case (Success(_), Success(_))   => Chain.empty
      case (Failure(ex), Success(_))  => Chain.one(ex)
      case (Success(_), Failure(ex))  => Chain.one(ex)
      case (Failure(kf), Failure(vf)) => Chain(kf, vf)
    }
    tell.writer(nj, log)
  }
}
