package com.github.chenharryhua.nanjin.kafka.serdes

import cats.Invariant
import fs2.kafka.{Key, KeyOrValue, Value}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.scalaland.chimney.Iso as ChimneyIso
import monocle.Iso as MonocleIso
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters.given

trait Unregistered[A] { outer =>
  protected def registerWith(srClient: SchemaRegistryClient): Serde[A]

  /*
   *  Transform
   */
  final def imap[B](f: A => B)(g: B => A): Unregistered[B] =
    new Unregistered[B] {
      def registerWith(srClient: SchemaRegistryClient): Serde[B] =
        summon[Invariant[Serde]].imap(outer.registerWith(srClient))(f)(g)
    }

  final def iso[B](isomorphism: MonocleIso[A, B]): Unregistered[B] =
    imap(isomorphism.get)(isomorphism.reverseGet)

  final def iso[B](isomorphism: ChimneyIso[A, B]): Unregistered[B] =
    imap(isomorphism.first.transform)(isomorphism.second.transform)

  // turn null into None
  final def optional(using Null <:< A): Unregistered[Option[A]] =
    imap(Option(_))(_.orNull)

  private trait IsKey[K]:
    def value: Boolean
  private given IsKey[Key] with
    override val value = true
  private given IsKey[Value] with
    override val value: Boolean = false

  private def register[B <: KeyOrValue](
    srClient: SchemaRegistryClient,
    props: Map[String, String]
  )(using isKey: IsKey[B]): Registered[B, A] =
    Registered[B, A](new Serde[A] {
      override val serializer: Serializer[A] =
        val ser = registerWith(srClient).serializer
        ser.configure(props.asJava, isKey.value)
        ser

      override val deserializer: Deserializer[A] =
        val deSer = registerWith(srClient).deserializer
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
