package com.github.chenharryhua.nanjin.spark.kafka

import alleycats.Empty
import cats.Bifunctor
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.sksamuel.avro4s.*
import fs2.kafka.ProducerRecord as Fs2ProducerRecord
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder}
import monocle.Optional
import monocle.macros.Lenses
import monocle.std.option.some
import org.apache.kafka.clients.producer.ProducerRecord
import shapeless.cachedImplicit

@AvroNamespace("nj.spark.kafka")
@AvroName("NJProducerRecord")
@Lenses
final case class NJProducerRecord[K, V](
  partition: Option[Int],
  offset: Option[Long], // for sort
  timestamp: Option[Long],
  key: Option[K],
  value: Option[V]) {

  def withPartition(pt: Int): NJProducerRecord[K, V]  = NJProducerRecord.partition.set(Some(pt))(this)
  def withTimestamp(ts: Long): NJProducerRecord[K, V] = NJProducerRecord.timestamp.set(Some(ts))(this)
  def withKey(k: K): NJProducerRecord[K, V]           = NJProducerRecord.key.set(Some(k))(this)
  def withValue(v: V): NJProducerRecord[K, V]         = NJProducerRecord.value.set(Some(v))(this)

  def noPartition: NJProducerRecord[K, V] = NJProducerRecord.partition.set(None)(this)
  def noTimestamp: NJProducerRecord[K, V] = NJProducerRecord.timestamp.set(None)(this)

  def noMeta: NJProducerRecord[K, V] =
    NJProducerRecord.timestamp[K, V].set(None).andThen(NJProducerRecord.partition[K, V].set(None))(this)

  def modifyKey(f: K => K): NJProducerRecord[K, V] =
    NJProducerRecord.key.modify((_: Option[K]).map(f))(this)

  def modifyValue(f: V => V): NJProducerRecord[K, V] =
    NJProducerRecord.value.modify((_: Option[V]).map(f))(this)

  @SuppressWarnings(Array("AsInstanceOf"))
  def toFs2ProducerRecord(topicName: String): Fs2ProducerRecord[K, V] = {
    val pr = Fs2ProducerRecord(topicName, key.getOrElse(null.asInstanceOf[K]), value.getOrElse(null.asInstanceOf[V]))
    (partition, timestamp) match {
      case (None, None)       => pr
      case (Some(p), None)    => pr.withPartition(p)
      case (None, Some(t))    => pr.withTimestamp(t)
      case (Some(p), Some(t)) => pr.withPartition(p).withTimestamp(t)
    }
  }

  @SuppressWarnings(Array("AsInstanceOf"))
  def toProducerRecord(topicName: String): ProducerRecord[K, V] = {
    val p = partition.map(x => java.lang.Integer.valueOf(x)).orNull
    val t = timestamp.map(x => java.lang.Long.valueOf(x)).orNull
    val k = key.getOrElse(null.asInstanceOf[K])
    val v = value.getOrElse(null.asInstanceOf[V])

    new ProducerRecord[K, V](topicName, p, t, k, v)
  }
}

object NJProducerRecord {
  def optionalKey[K, V]: Optional[NJProducerRecord[K, V], K]   = NJProducerRecord.key[K, V].composePrism(some)
  def optionalValue[K, V]: Optional[NJProducerRecord[K, V], V] = NJProducerRecord.value[K, V].composePrism(some)

  def apply[K, V](pr: ProducerRecord[Option[K], Option[V]]): NJProducerRecord[K, V] =
    NJProducerRecord(Option(pr.partition), None, Option(pr.timestamp), pr.key, pr.value)

  def apply[K, V](k: K, v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, None, Option(k), Option(v))

  def apply[K, V](v: V): NJProducerRecord[K, V] =
    NJProducerRecord(None, None, None, None, Option(v))

  def avroCodec[K, V](keyCodec: AvroCodec[K], valCodec: AvroCodec[V]): AvroCodec[NJProducerRecord[K, V]] = {
    implicit val schemaForKey: SchemaFor[K]  = keyCodec.schemaFor
    implicit val schemaForVal: SchemaFor[V]  = valCodec.schemaFor
    implicit val keyDecoder: Decoder[K]      = keyCodec.avroDecoder
    implicit val valDecoder: Decoder[V]      = valCodec.avroDecoder
    implicit val keyEncoder: Encoder[K]      = keyCodec.avroEncoder
    implicit val valEncoder: Encoder[V]      = valCodec.avroEncoder
    val s: SchemaFor[NJProducerRecord[K, V]] = cachedImplicit
    val d: Decoder[NJProducerRecord[K, V]]   = cachedImplicit
    val e: Encoder[NJProducerRecord[K, V]]   = cachedImplicit
    AvroCodec[NJProducerRecord[K, V]](s, d.withSchema(s), e.withSchema(s))
  }

  def avroCodec[K, V](topicDef: TopicDef[K, V]): AvroCodec[NJProducerRecord[K, V]] =
    avroCodec(topicDef.serdeOfKey.avroCodec, topicDef.serdeOfVal.avroCodec)

  implicit def jsonEncoder[K, V](implicit
    jck: JsonEncoder[K],
    jcv: JsonEncoder[V]): JsonEncoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveEncoder[NJProducerRecord[K, V]]

  implicit def jsonDecoder[K, V](implicit
    jck: JsonDecoder[K],
    jcv: JsonDecoder[V]): JsonDecoder[NJProducerRecord[K, V]] =
    io.circe.generic.semiauto.deriveDecoder[NJProducerRecord[K, V]]

  implicit def emptyNJProducerRecord[K, V]: Empty[NJProducerRecord[K, V]] =
    new Empty[NJProducerRecord[K, V]] {
      override val empty: NJProducerRecord[K, V] = NJProducerRecord(None, None, None, None, None)
    }

  implicit val bifunctorNJProducerRecord: Bifunctor[NJProducerRecord] =
    new Bifunctor[NJProducerRecord] {

      override def bimap[A, B, C, D](fab: NJProducerRecord[A, B])(f: A => C, g: B => D): NJProducerRecord[C, D] =
        fab.copy(key = fab.key.map(f), value = fab.value.map(g))
    }
}
