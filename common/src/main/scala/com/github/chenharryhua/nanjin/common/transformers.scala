package com.github.chenharryhua.nanjin.common

import cats.arrow.Arrow
import io.circe.{Decoder, Encoder}
import io.scalaland.chimney.Transformer
import io.scalaland.enumz.Enum
import monocle.Iso
import shapeless.Witness

import java.sql.Timestamp
import java.time.Instant

object transformers extends TransformersTrait with ReverseTransformers

trait TransformersTrait {
  implicit def enumCirceEncoder[E <: Enumeration](implicit w: Witness.Aux[E]): Encoder[E#Value] =
    Encoder.encodeEnumeration(w.value)

  implicit def enumCirceDecoder[E <: Enumeration](implicit w: Witness.Aux[E]): Decoder[E#Value] =
    Decoder.decodeEnumeration(w.value)

  implicit final def str2Enum[E](implicit ev: Enum[E]): Transformer[String, E] =
    (src: String) => ev.withNameInsensitive(src)

  implicit final def int2Enum[E](implicit ev: Enum[E]): Transformer[Int, E] =
    (src: Int) => ev.withIndex(src)

  implicit final def aISOb[A, B](implicit iso: Iso[A, B]): Transformer[A, B] =
    (src: A) => iso.get(src)

  implicit final def transformToTimestamp[A](implicit
    trans: Transformer[A, Instant]): Transformer[A, Timestamp] =
    (src: A) => Timestamp.from(trans.transform(src))

  implicit final val transformerArrow: Arrow[Transformer] =
    new Arrow[Transformer] {

      override def first[A, B, C](fa: Transformer[A, B]): Transformer[(A, C), (B, C)] =
        (src: (A, C)) => (fa.transform(src._1), src._2)

      override def lift[A, B](f: A => B): Transformer[A, B] = (src: A) => f(src)

      override def compose[A, B, C](f: Transformer[B, C], g: Transformer[A, B]): Transformer[A, C] =
        (src: A) => f.transform(g.transform(src))

    }
}

trait ReverseTransformers {

  implicit final def enum2Str[E](implicit ev: Enum[E]): Transformer[E, String] =
    (src: E) => ev.getName(src)

  implicit final def enum2Int[E](implicit ev: Enum[E]): Transformer[E, Int] =
    (src: E) => ev.getIndex(src)

  implicit final def bISOa[A, B](implicit iso: Iso[A, B]): Transformer[B, A] =
    (src: B) => iso.reverseGet(src)

  implicit final def transformFromTimestamp[A](implicit
    trans: Transformer[Instant, A]): Transformer[Timestamp, A] =
    (src: Timestamp) => trans.transform(src.toInstant)

}
