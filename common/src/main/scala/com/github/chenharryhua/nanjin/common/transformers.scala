package com.github.chenharryhua.nanjin.common

import java.sql.Timestamp
import java.time.Instant

import cats.arrow.Profunctor
import io.scalaland.chimney.Transformer
import io.scalaland.enumz.Enum
import monocle.Iso

object transformers extends ReverseTransformers {

  implicit def str2Enum[E](implicit ev: Enum[E]): Transformer[String, E] =
    (src: String) => ev.withNameInsensitive(src)

  implicit def int2Enum[E](implicit ev: Enum[E]): Transformer[Int, E] =
    (src: Int) => ev.withIndex(src)

  implicit def aISOb[A, B](implicit iso: Iso[A, B]): Transformer[A, B] =
    (src: A) => iso.get(src)

  implicit def transformToTimestamp[A](implicit
    trans: Transformer[A, Instant]): Transformer[A, Timestamp] =
    (src: A) => Timestamp.from(trans.transform(src))

  implicit val transformerProfunctor: Profunctor[Transformer] =
    new Profunctor[Transformer] {

      override def dimap[A, B, C, D](fab: Transformer[A, B])(f: C => A)(
        g: B => D): Transformer[C, D] = (src: C) => g(fab.transform(f(src)))
    }
}

trait ReverseTransformers {

  implicit def enum2Str[E](implicit ev: Enum[E]): Transformer[E, String] =
    (src: E) => ev.getName(src)

  implicit def enum2Int[E](implicit ev: Enum[E]): Transformer[E, Int] =
    (src: E) => ev.getIndex(src)

  implicit def bISOa[A, B](implicit iso: Iso[A, B]): Transformer[B, A] =
    (src: B) => iso.reverseGet(src)

  implicit def transformFromTimestamp[A](implicit
    trans: Transformer[Instant, A]): Transformer[Timestamp, A] =
    (src: Timestamp) => trans.transform(src.toInstant)

}
