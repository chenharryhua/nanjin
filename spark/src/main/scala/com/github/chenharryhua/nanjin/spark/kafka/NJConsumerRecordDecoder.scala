package com.github.chenharryhua.nanjin.spark.kafka

import cats.data.Chain
import cats.mtl.Tell
import cats.syntax.all._
import com.github.chenharryhua.nanjin.messages.kafka._
import org.apache.kafka.common.serialization.Deserializer

import scala.util.{Failure, Success, Try}

final class NJConsumerRecordDecoder[F[_], K, V](
  topicName: String,
  keyDeserializer: Deserializer[K],
  valDeserializer: Deserializer[V])
    extends Serializable {

  def decode[G[_, _]](gaa: G[Array[Byte], Array[Byte]])(implicit
    cm: NJConsumerMessage[G],
    tell: Tell[F, Chain[Throwable]]): F[NJConsumerRecord[K, V]] = {
    val cr = cm.lens.get(gaa)
    val k  = Option(cr.key).traverse(dk => Try(keyDeserializer.deserialize(topicName, dk)))
    val v  = Option(cr.value).traverse(dv => Try(valDeserializer.deserialize(topicName, dv)))
    val nj = NJConsumerRecord(cr.bimap(_ => k.toOption.flatten, _ => v.toOption.flatten))
    val log = (k, v) match {
      case (Success(kv), Success(vv)) => Chain.empty
      case (Failure(ex), Success(_))  => Chain.one(ex)
      case (Success(_), Failure(ex))  => Chain.one(ex)
      case (Failure(kf), Failure(vf)) => Chain(kf, vf)
    }
    tell.writer(nj, log)
  }

  def decode(cr: NJConsumerRecord[Array[Byte], Array[Byte]])(implicit
    tell: Tell[F, Chain[Throwable]]): F[NJConsumerRecord[K, V]] = {
    val k  = cr.key.traverse(k => Try(keyDeserializer.deserialize(topicName, k)))
    val v  = cr.value.traverse(v => Try(valDeserializer.deserialize(topicName, v)))
    val nj = cr.bimap(_ => k.toOption.flatten, _ => v.toOption.flatten).flatten[K, V]

    val log = (k, v) match {
      case (Success(kv), Success(vv)) => Chain.empty
      case (Failure(ex), Success(_))  => Chain.one(ex)
      case (Success(_), Failure(ex))  => Chain.one(ex)
      case (Failure(kf), Failure(vf)) => Chain(kf, vf)
    }
    tell.writer(nj, log)
  }
}
