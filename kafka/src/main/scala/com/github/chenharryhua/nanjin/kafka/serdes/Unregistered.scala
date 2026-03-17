package com.github.chenharryhua.nanjin.kafka.serdes

import cats.Invariant
import fs2.kafka.{Key, KeyOrValue, Value}
import io.scalaland.chimney.Iso as ChimneyIso
import monocle.Iso as MonocleIso
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import scala.jdk.CollectionConverters.given

trait Unregistered[A] { outer =>
  protected def unregistered: Serde[A]

  /*
   *  Transform
   */
  final def imap[B](f: A => B)(g: B => A): Unregistered[B] =
    new Unregistered[B] {
      def unregistered: Serde[B] = summon[Invariant[Serde]].imap(outer.unregistered)(f)(g)
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

  private def register[B <: KeyOrValue](props: Map[String, String])(using isKey: IsKey[B]): Registered[B, A] =
    Registered[B, A] {
      new Serde[A] {
        override def serializer: Serializer[A] =
          val ser = unregistered.serializer
          ser.configure(props.asJava, isKey.value)
          ser

        override def deserializer: Deserializer[A] =
          val deser = unregistered.deserializer
          deser.configure(props.asJava, isKey.value)
          deser
      }
    }

  /*
   * Transition
   */

  // registered as key of a topic
  final def asKey(props: Map[String, String]): Registered[Key, A] =
    register[Key](props)

  // registered as value of a topic
  final def asValue(props: Map[String, String]): Registered[Value, A] =
    register[Value](props)
}
