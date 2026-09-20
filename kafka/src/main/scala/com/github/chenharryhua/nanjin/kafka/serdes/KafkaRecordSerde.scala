package com.github.chenharryhua.nanjin.kafka.serdes

import cats.syntax.bifunctor.given
import cats.syntax.bitraverse.given
import cats.{Bifunctor, Bitraverse}
import com.github.chenharryhua.nanjin.kafka.TopicName
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.util.{Success, Try}

/** A single-sided Kafka `Serde` bound to a topic, exposing plain `serialize`/`deserialize`.
  *
  * Both directions carry the topic name into the underlying Kafka serializer/deserializer (some Confluent
  * serdes are topic-sensitive). `deserialize` propagates whatever the underlying deserializer does on `null`
  * or malformed bytes (it does not guard them).
  */
final class KafkaSerde[A] private[kafka] (val serde: Serde[A], topicName: TopicName) {
  private val ser: Serializer[A] = serde.serializer()

  /** Serialize `a` to bytes for this topic. */
  def serialize(a: A): Array[Byte] = ser.serialize(topicName.value, a)

  private val deser: Deserializer[A] = serde.deserializer()

  /** Deserialize bytes into `A` for this topic. */
  def deserialize(ab: Array[Byte]): A = deser.deserialize(topicName.value, ab)
}

/** Raised by the `try*`/`optional*` deserializers when the key or value bytes are `null`. */
case object DeserializeNull extends Exception("deserialize null")

/** Key/value serde for a topic, offering several strategies for handling `null` and malformed bytes.
  *
  * All methods operate over a bifunctor/bitraverse `G[_, _]` (e.g. a tuple or `Either`) holding the raw key
  * and value bytes, applying the key serde to the first side and the value serde to the second. The variants
  * differ only in error handling:
  *   - plain `deserialize`/`serialize`: no guarding, the underlying serde's behavior on `null`/bad bytes
  *     propagates.
  *   - `deserialize{Key,Value}`: wrap the named side in `Option` (`null` becomes `None`).
  *   - `try*`: capture `null` (as `DeserializeNull`) and deserialization exceptions in `Try`.
  *   - `optionalDeserialize`: capture both `null` and failures as `None`.
  */
abstract class KafkaRecordSerde[K, V] private[kafka] (keySerde: KafkaSerde[K], valSerde: KafkaSerde[V]) {

  /** Deserialize both sides, propagating any underlying error. */
  def deserialize[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[K, V] =
    data.bimap(keySerde.deserialize, valSerde.deserialize)

  /** Deserialize only the key (as `Option`, `null` -> `None`), leaving the value bytes untouched. */
  def deserializeKey[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[Option[K], Array[Byte]] =
    data.bimap(Option(_).map(keySerde.deserialize), identity)

  /** Deserialize only the value (as `Option`, `null` -> `None`), leaving the key bytes untouched. */
  def deserializeValue[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[Array[Byte], Option[V]] =
    data.bimap(identity, Option(_).map(valSerde.deserialize))

  /** Deserialize both sides into `Try`, capturing a `null` side as `DeserializeNull` and any deserialization
    * exception as a `Failure`.
    */
  def tryDeserializeKeyValue[G[_, _]: Bifunctor](data: G[Array[Byte], Array[Byte]]): G[Try[K], Try[V]] =
    data.bimap(
      nk => Option(nk).toRight(DeserializeNull).toTry.flatMap(k => Try(keySerde.deserialize(k))),
      nv => Option(nv).toRight(DeserializeNull).toTry.flatMap(v => Try(valSerde.deserialize(v)))
    )

  /** Deserialize both sides, collapsing the pair into a single `Try` that succeeds only if both sides do. */
  def tryDeserialize[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): Try[G[K, V]] =
    tryDeserializeKeyValue(data).bitraverse(identity, identity)

  /** Deserialize only the value into `Try` (key bytes pass through), succeeding only if the value does. */
  def tryDeserializeValue[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): Try[G[Array[Byte], V]] =
    data.bitraverse(
      Success(_),
      nv => Option(nv).toRight(DeserializeNull).toTry.flatMap(v => Try(valSerde.deserialize(v)))
    )

  /** Deserialize only the key into `Try` (value bytes pass through), succeeding only if the key does. */
  def tryDeserializeKey[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): Try[G[K, Array[Byte]]] =
    data.bitraverse(
      nk => Option(nk).toRight(DeserializeNull).toTry.flatMap(k => Try(keySerde.deserialize(k))),
      Success(_)
    )

  /** Deserialize both sides leniently: a `null` or a deserialization failure on either side becomes `None`.
    */
  def optionalDeserialize[G[_, _]: Bitraverse](data: G[Array[Byte], Array[Byte]]): G[Option[K], Option[V]] =
    data.bimap(
      nk => Option(nk).flatMap(k => Try(keySerde.deserialize(k)).toOption),
      nv => Option(nv).flatMap(v => Try(valSerde.deserialize(v)).toOption)
    )

  /*
   * Serialize
   */

  /** Serialize the key to bytes. */
  def serializeKey(k: K): Array[Byte] = keySerde.serialize(k)

  /** Serialize the value to bytes. */
  def serializeValue(v: V): Array[Byte] = valSerde.serialize(v)

  /** Serialize both sides to bytes. */
  def serialize[G[_, _]: Bifunctor](data: G[K, V]): G[Array[Byte], Array[Byte]] =
    data.bimap(serializeKey, serializeValue)
}
