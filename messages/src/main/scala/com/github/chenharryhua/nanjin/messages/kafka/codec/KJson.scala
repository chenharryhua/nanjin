package com.github.chenharryhua.nanjin.messages.kafka.codec

import cats.{Distributive, Eq, Functor, Show}
import io.circe.Decoder.Result
import io.circe.syntax.*
import io.circe.{Codec as JsonCodec, Decoder as JsonDecoder, Encoder as JsonEncoder, HCursor, Json}
import monocle.Iso

final class KJson[A] private (val value: A) extends Serializable {
  @SuppressWarnings(Array("IsInstanceOf"))
  private def canEqual(a: Any): Boolean = a.isInstanceOf[KJson[?]]

  override def equals(that: Any): Boolean =
    that match {
      // equality is symmetric
      case that: KJson[?] => that.canEqual(this) && this.value == that.value
      case _              => false
    }
  override def hashCode: Int = value.hashCode()

  override def toString: String = s"KJson(value=${value.toString})"
}

object KJson {
  def apply[A](a: A): KJson[A] = new KJson[A](a)

  implicit def showKafkaJson[A: JsonEncoder]: Show[KJson[A]] =
    (t: KJson[A]) => s"""KJson(value=${Option(t.value).map(_.asJson.noSpaces).getOrElse("null")})"""

  implicit def eqKJson[A: Eq]: Eq[KJson[A]] = (x: KJson[A], y: KJson[A]) => Eq[A].eqv(x.value, y.value)

  implicit def jsonCodec[A: JsonEncoder: JsonDecoder]: JsonCodec[KJson[A]] =
    new JsonCodec[KJson[A]] {
      override def apply(a: KJson[A]): Json = JsonEncoder[A].apply(a.value)
      override def apply(c: HCursor): Result[KJson[A]] = JsonDecoder[A].apply(c).map(KJson[A])
    }

  implicit val distributiveKJson: Distributive[KJson] = new Distributive[KJson] {

    override def distribute[G[_], A, B](ga: G[A])(f: A => KJson[B])(implicit ev: Functor[G]): KJson[G[B]] = {
      val gb = ev.map(ga)(x => f(x).value)
      KJson(gb)
    }

    override def map[A, B](fa: KJson[A])(f: A => B): KJson[B] = KJson(f(fa.value))
  }

  implicit def isoKJson[A]: Iso[KJson[A], A] = Iso[KJson[A], A](_.value)(KJson(_))
}
