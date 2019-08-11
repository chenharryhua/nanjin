package com.github.chenharryhua.nanjin.sparkafka
import com.github.chenharryhua.nanjin.kafka.KafkaTopic

import scala.reflect.macros.blackbox
import scala.language.experimental.macros

trait MyDecoder[K, V] extends Serializable {
  def key(data: Array[Byte]): K
  def value(data: Array[Byte]): V
}

final class SparkafkaMacro(val c: blackbox.Context) {

  def impl[F[_], K: c.WeakTypeTag, V: c.WeakTypeTag](topic: c.Expr[KafkaTopic[F, K, V]]) = {
    import c.universe._
    val k = weakTypeOf[K]
    val v = weakTypeOf[V]
    println(topic)
    q"""
       new MyDecoder[$k,$v]{
          override def key(data:Array[Byte]): $k = $topic.keyIso.get(data)
          override def value(data:Array[Byte]): $v = $topic.valueIso.get(data)
       }
     """
  }
}

object SparkafkaMacro {

  def decoder[F[_], K, V](topic: KafkaTopic[F, K, V]): MyDecoder[K, V] =
    macro SparkafkaMacro.impl[F, K, V]
}
