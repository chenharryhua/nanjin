package com.github.chenharryhua.nanjin.common

import java.sql.Timestamp
import java.time.Instant

import io.scalaland.chimney.Transformer
import io.scalaland.enumz.Enum
import monocle.Iso

object transformers extends ReverseTransformers {

  implicit def str2Enum[E](implicit ev: Enum[E]): Transformer[String, E] =
    (src: String) => ev.withNameInsensitive(src)

  implicit def aISOb[A, B](implicit iso: Iso[A, B]): Transformer[A, B] =
    (src: A) => iso.get(src)

  implicit def transformToTimestamp[A](implicit
    trans: Transformer[A, Instant]): Transformer[A, Timestamp] =
    (src: A) => Timestamp.from(trans.transform(src))

}

trait ReverseTransformers {

  implicit def enum2Str[E](implicit ev: Enum[E]): Transformer[E, String] =
    (src: E) => ev.getName(src)

  implicit def bISOa[A, B](implicit iso: Iso[A, B]): Transformer[B, A] =
    (src: B) => iso.reverseGet(src)

  implicit def transformFromTimestamp[A](implicit
    trans: Transformer[Instant, A]): Transformer[Timestamp, A] =
    (src: Timestamp) => trans.transform(src.toInstant)

}
