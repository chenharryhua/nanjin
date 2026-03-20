package com.github.chenharryhua.nanjin.kafka.serdes

import fs2.kafka.{Key, KeyOrValue, Value}
import io.circe.Json
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters.given

trait Unregistered[A] { outer =>
  protected def registerWith(srClient: SchemaRegistryClient): Serde[A]

  /*
   *  Transform
   */
  final def emap[B](f: A => B)(g: B => A): Unregistered[B] =
    new Unregistered[B] {
      protected def registerWith(srClient: SchemaRegistryClient): Serde[B] =
        new Serde[B] {
          private val serdeA: Serde[A] = outer.registerWith(srClient)

          override val serializer: Serializer[B] = new Serializer[B] {
            private val ser = serdeA.serializer
            override def serialize(topic: String, data: B): Array[Byte] =
              ser.serialize(topic, g(data))
            override def serialize(topic: String, headers: Headers, data: B): Array[Byte] =
              ser.serialize(topic, headers, g(data))
            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)
            override def close(): Unit = ser.close()
          }

          override val deserializer: Deserializer[B] = new Deserializer[B] {
            private val deSer = serdeA.deserializer
            override def deserialize(topic: String, data: Array[Byte]): B =
              f(deSer.deserialize(topic, data))
            override def deserialize(topic: String, headers: Headers, data: Array[Byte]): B =
              f(deSer.deserialize(topic, headers, data))
            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)
            override def close(): Unit = deSer.close()
          }
        }
    }

  final def become[B](using b: BiTransform[A, B]): Unregistered[B] =
    emap(b.to)(b.from)
  final def circe(using b: BiTransform[A, Json]): Unregistered[Json] =
    emap(b.to)(b.from)

  // turn null into None
  final def optional(using Null <:< A): Unregistered[Option[A]] =
    emap(Option(_))(_.orNull)

  private trait IsKey[K]:
    def value: Boolean
  private given IsKey[Key] with
    override val value = true
  private given IsKey[Value] with
    override val value: Boolean = false

  private def register[KV <: KeyOrValue](
    srClient: SchemaRegistryClient,
    props: Map[String, String]
  )(using isKey: IsKey[KV]): Registered[KV, A] =
    Registered[KV, A](new Serde[A] {
      private val serde: Serde[A] = outer.registerWith(srClient)
      override lazy val serializer: Serializer[A] =
        val ser: Serializer[A] = serde.serializer
        ser.configure(props.asJava, isKey.value)
        ser

      override lazy val deserializer: Deserializer[A] =
        val deSer: Deserializer[A] = serde.deserializer
        deSer.configure(props.asJava, isKey.value)
        deSer
    })

  /*
   * Transition
   */

  // registered as key of a topic
  final def asKey(srClient: SchemaRegistryClient, props: Map[String, String]): Registered[Key, A] =
    register[Key](srClient, props)

  // registered as value of a topic
  final def asValue(srClient: SchemaRegistryClient, props: Map[String, String]): Registered[Value, A] =
    register[Value](srClient, props)
}
