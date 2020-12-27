package com.github.chenharryhua.nanjin.spark.kafka

import cats.implicits.catsSyntaxTuple2Semigroupal
import cats.kernel.PartialOrder
import cats.syntax.eq._
import cats.{Applicative, Bifunctor, Bitraverse, Eval, Show}
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.sksamuel.avro4s.{AvroDoc, Decoder, Encoder, SchemaFor}
import frameless.TypedEncoder
import io.circe.generic.auto._
import io.circe.{Json, Encoder => JsonEncoder}
import io.scalaland.chimney.dsl._
import monocle.macros.Lenses
import org.apache.kafka.clients.consumer.ConsumerRecord
import shapeless.cachedImplicit

import java.time.Instant

/** compatible with spark kafka streaming
  * https://spark.apache.org/docs/3.0.1/structured-streaming-kafka-integration.html
  */
sealed trait NJConsumerRecord[K, V] {
  self =>
  val partition: Int
  val offset: Long
  val timestamp: Long
  val key: K
  val value: V
  val topic: String
  val timestampType: Int

  final def metaInfo: String =
    s"Meta(topic=$topic,partition=$partition,offset=$offset,ts=${Instant.ofEpochMilli(timestamp)})"

  final def display(k: Show[K], v: Show[V]): String =
    s"CR($metaInfo,key=${k.show(key)},value=${v.show(value)})"

}

object NJConsumerRecord {

  implicit def showNJConsumerRecord[A, K, V](implicit
    ev: A <:< NJConsumerRecord[K, V],
    k: Show[K],
    v: Show[V]): Show[A] = _.display(k, v)
}

@Lenses @AvroDoc("kafka record, optional Key and Value")
final case class OptionalKV[K, V](
  @AvroDoc("kafka partition") partition: Int,
  @AvroDoc("kafka offset") offset: Long,
  @AvroDoc("kafka timestamp in millisecond") timestamp: Long,
  @AvroDoc("kafka key") key: Option[K],
  @AvroDoc("kafka value") value: Option[V],
  @AvroDoc("kafka topic") topic: String,
  @AvroDoc("kafka timestamp type") timestampType: Int)
    extends NJConsumerRecord[Option[K], Option[V]] {

  def newKey[K2](key: Option[K2]): OptionalKV[K2, V]     = copy(key = key)
  def newValue[V2](value: Option[V2]): OptionalKV[K, V2] = copy(value = value)

  def flatten[K2, V2](implicit
    evK: K <:< Option[K2],
    evV: V <:< Option[V2]
  ): OptionalKV[K2, V2] =
    copy(key = key.flatten, value = value.flatten)

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(offset), Some(timestamp), key, value)

  def toCompulsoryK: Option[CompulsoryK[K, V]] =
    key.map(k => this.into[CompulsoryK[K, V]].withFieldConst(_.key, k).transform)

  def toCompulsoryV: Option[CompulsoryV[K, V]] =
    value.map(v => this.into[CompulsoryV[K, V]].withFieldConst(_.value, v).transform)

  def toCompulsoryKV: Option[CompulsoryKV[K, V]] =
    (key, value).mapN { case (k, v) =>
      this.into[CompulsoryKV[K, V]].withFieldConst(_.key, k).withFieldConst(_.value, v).transform
    }

  def asJson(implicit k: JsonEncoder[K], v: JsonEncoder[V]): Json =
    JsonEncoder[OptionalKV[K, V]].apply(this)
}

object OptionalKV {

  def apply[K, V](cr: ConsumerRecord[Option[K], Option[V]]): OptionalKV[K, V] =
    OptionalKV(cr.partition, cr.offset, cr.timestamp, cr.key, cr.value, cr.topic, cr.timestampType.id)

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[OptionalKV[K, V]] = {
    implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]     = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]     = valCodec.avroEncoder
    val s: SchemaFor[OptionalKV[K, V]]      = cachedImplicit
    val d: Decoder[OptionalKV[K, V]]        = cachedImplicit
    val e: Encoder[OptionalKV[K, V]]        = cachedImplicit
    AvroCodec[OptionalKV[K, V]](s, d, e)
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): AvroCodec[OptionalKV[K, V]] =
    avroCodec(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  def ate[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[OptionalKV[K, V]] = {
    val ote: TypedEncoder[OptionalKV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder[OptionalKV[K, V]](ote, avroCodec(keyCodec, valCodec))
  }

  def ate[K, V](
    topicDef: TopicDef[K, V])(implicit tek: TypedEncoder[K], tev: TypedEncoder[V]): AvroTypedEncoder[OptionalKV[K, V]] =
    ate(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  implicit val bifunctorOptionalKV: Bifunctor[OptionalKV] =
    new Bifunctor[OptionalKV] {

      override def bimap[A, B, C, D](fab: OptionalKV[A, B])(f: A => C, g: B => D): OptionalKV[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }

  implicit def partialOrderOptionlKV[K, V]: PartialOrder[OptionalKV[K, V]] =
    (x: OptionalKV[K, V], y: OptionalKV[K, V]) =>
      if (x.partition === y.partition) {
        if (x.offset < y.offset) -1.0 else if (x.offset > y.offset) 1.0 else 0.0
      } else Double.NaN
}

@AvroDoc("kafka record, optional Key and compulsory Value")
@Lenses
final case class CompulsoryV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecord[Option[K], V] {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(offset), Some(timestamp), key, Some(value))

  def toCompulsoryKV: Option[CompulsoryKV[K, V]] =
    key.map(k => this.into[CompulsoryKV[K, V]].withFieldConst(_.key, k).transform)

  def toOptionalKV: OptionalKV[K, V] =
    this.into[OptionalKV[K, V]].withFieldConst(_.value, Some(value)).transform

  def asJson(implicit k: JsonEncoder[K], v: JsonEncoder[V]): Json =
    JsonEncoder[CompulsoryV[K, V]].apply(this)

}

object CompulsoryV {

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[CompulsoryV[K, V]] = {
    implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]     = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]     = valCodec.avroEncoder
    val s: SchemaFor[CompulsoryV[K, V]]     = cachedImplicit
    val d: Decoder[CompulsoryV[K, V]]       = cachedImplicit
    val e: Encoder[CompulsoryV[K, V]]       = cachedImplicit
    AvroCodec[CompulsoryV[K, V]](s, d, e)
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): AvroCodec[CompulsoryV[K, V]] =
    avroCodec(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  def ate[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryV[K, V]] = {
    val ote: TypedEncoder[CompulsoryV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder[CompulsoryV[K, V]](ote, avroCodec(keyCodec, valCodec))
  }

  def ate[K, V](topicDef: TopicDef[K, V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryV[K, V]] =
    ate(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  implicit val bifunctorCompulsoryV: Bifunctor[CompulsoryV] =
    new Bifunctor[CompulsoryV] {

      override def bimap[A, B, C, D](fab: CompulsoryV[A, B])(f: A => C, g: B => D): CompulsoryV[C, D] =
        fab.copy(key = fab.key.map(f), value = g(fab.value))
    }
}

@AvroDoc("kafka record, compulsory Key and optional Value")
@Lenses
final case class CompulsoryK[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: Option[V],
  topic: String,
  timestampType: Int)
    extends NJConsumerRecord[K, Option[V]] {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(offset), Some(timestamp), Some(key), value)

  def toCompulsoryKV: Option[CompulsoryKV[K, V]] =
    value.map(v => this.into[CompulsoryKV[K, V]].withFieldConst(_.value, v).transform)

  def toOptionalKV: OptionalKV[K, V] =
    this.into[OptionalKV[K, V]].withFieldConst(_.key, Some(key)).transform

  def asJson(implicit k: JsonEncoder[K], v: JsonEncoder[V]): Json =
    JsonEncoder[CompulsoryK[K, V]].apply(this)

}

object CompulsoryK {

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[CompulsoryK[K, V]] = {
    implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]     = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]     = valCodec.avroEncoder
    val s: SchemaFor[CompulsoryK[K, V]]     = cachedImplicit
    val d: Decoder[CompulsoryK[K, V]]       = cachedImplicit
    val e: Encoder[CompulsoryK[K, V]]       = cachedImplicit
    AvroCodec[CompulsoryK[K, V]](s, d, e)
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): AvroCodec[CompulsoryK[K, V]] =
    avroCodec(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  def ate[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryK[K, V]] = {
    val ote: TypedEncoder[CompulsoryK[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder[CompulsoryK[K, V]](ote, avroCodec(keyCodec, valCodec))
  }

  def ate[K, V](topicDef: TopicDef[K, V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryK[K, V]] =
    ate(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  implicit val bifunctorCompulsoryK: Bifunctor[CompulsoryK] =
    new Bifunctor[CompulsoryK] {

      override def bimap[A, B, C, D](fab: CompulsoryK[A, B])(f: A => C, g: B => D): CompulsoryK[C, D] =
        fab.copy(key = f(fab.key), value = fab.value.map(g))
    }
}

@AvroDoc("kafka record, compulsory Key and Value")
@Lenses
final case class CompulsoryKV[K, V](
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: K,
  value: V,
  topic: String,
  timestampType: Int)
    extends NJConsumerRecord[K, V] {

  def toNJProducerRecord: NJProducerRecord[K, V] =
    NJProducerRecord[K, V](Some(partition), Some(offset), Some(timestamp), Some(key), Some(value))

  def toOptionalKV: OptionalKV[K, V] =
    this.into[OptionalKV[K, V]].withFieldConst(_.key, Some(key)).withFieldConst(_.value, Some(value)).transform

  def toCompulsoryK: CompulsoryK[K, V] =
    this.into[CompulsoryK[K, V]].withFieldConst(_.value, Some(value)).transform

  def toCompulsoryV: CompulsoryV[K, V] =
    this.into[CompulsoryV[K, V]].withFieldConst(_.key, Some(key)).transform

  def asJson(implicit k: JsonEncoder[K], v: JsonEncoder[V]): Json =
    JsonEncoder[CompulsoryKV[K, V]].apply(this)

}

object CompulsoryKV {

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[CompulsoryKV[K, V]] = {
    implicit val schemaForKey: SchemaFor[K] = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V] = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]     = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]     = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]     = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]     = valCodec.avroEncoder
    val s: SchemaFor[CompulsoryKV[K, V]]    = cachedImplicit
    val d: Decoder[CompulsoryKV[K, V]]      = cachedImplicit
    val e: Encoder[CompulsoryKV[K, V]]      = cachedImplicit
    AvroCodec[CompulsoryKV[K, V]](s, d, e)
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): AvroCodec[CompulsoryKV[K, V]] =
    avroCodec(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  def ate[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryKV[K, V]] = {
    val ote: TypedEncoder[CompulsoryKV[K, V]] = shapeless.cachedImplicit
    AvroTypedEncoder[CompulsoryKV[K, V]](ote, avroCodec(keyCodec, valCodec))
  }

  def ate[K, V](topicDef: TopicDef[K, V])(implicit
    tek: TypedEncoder[K],
    tev: TypedEncoder[V]): AvroTypedEncoder[CompulsoryKV[K, V]] =
    ate(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  implicit val bitraverseCompulsoryKV: Bitraverse[CompulsoryKV] =
    new Bitraverse[CompulsoryKV] {

      override def bimap[A, B, C, D](fab: CompulsoryKV[A, B])(f: A => C, g: B => D): CompulsoryKV[C, D] =
        fab.copy(key = f(fab.key), value = g(fab.value))

      override def bitraverse[G[_], A, B, C, D](fab: CompulsoryKV[A, B])(f: A => G[C], g: B => G[D])(implicit
        G: Applicative[G]): G[CompulsoryKV[C, D]] =
        G.map2(f(fab.key), g(fab.value))((k, v) => bimap(fab)(_ => k, _ => v))

      override def bifoldLeft[A, B, C](fab: CompulsoryKV[A, B], c: C)(f: (C, A) => C, g: (C, B) => C): C =
        g(f(c, fab.key), fab.value)

      override def bifoldRight[A, B, C](fab: CompulsoryKV[A, B], c: Eval[C])(
        f: (A, Eval[C]) => Eval[C],
        g: (B, Eval[C]) => Eval[C]): Eval[C] = g(fab.value, f(fab.key, c))
    }
}
