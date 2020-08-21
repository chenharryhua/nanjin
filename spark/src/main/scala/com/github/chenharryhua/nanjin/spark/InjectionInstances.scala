package com.github.chenharryhua.nanjin.spark

import java.sql.{Date, Timestamp}

import cats.Order
import frameless.{Injection, SQLDate, SQLTimestamp}
import io.circe.{Decoder, Encoder}
import monocle.Iso
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import shapeless.Witness

private[spark] trait InjectionInstances extends Serializable {

  implicit val isoSQLDate: Iso[Date, SQLDate] =
    Iso[Date, SQLDate](a => SQLDate(DateTimeUtils.fromJavaDate(a)))(b =>
      DateTimeUtils.toJavaDate(b.days))

  implicit val isoSQLTimestamp: Iso[Timestamp, SQLTimestamp] =
    Iso[Timestamp, SQLTimestamp](a => SQLTimestamp(DateTimeUtils.fromJavaTimestamp(a)))(b =>
      DateTimeUtils.toJavaTimestamp(b.us))

  implicit val oderSQLDate: Order[SQLDate] =
    (x: SQLDate, y: SQLDate) => x.days.compareTo(y.days)

  implicit val orderSQLTimestamp: Order[SQLTimestamp] =
    (x: SQLTimestamp, y: SQLTimestamp) => x.us.compareTo(y.us)

  implicit def isoInjection[A, B](implicit iso: Iso[A, B]): Injection[A, B] =
    Injection[A, B](iso.get, iso.reverseGet)

  implicit def enumToStringInjection[E <: Enumeration](implicit
    w: Witness.Aux[E]): Injection[E#Value, String] =
    Injection(_.toString, x => w.value.withName(x))

  implicit def enumCirceEncoder[E <: Enumeration](implicit w: Witness.Aux[E]): Encoder[E#Value] =
    Encoder.encodeEnumeration(w.value)

  implicit def enumCirceDecoder[E <: Enumeration](implicit w: Witness.Aux[E]): Decoder[E#Value] =
    Decoder.decodeEnumeration(w.value)

  implicit def orderScalaEnum[E <: Enumeration](implicit
    w: shapeless.Witness.Aux[E]): Order[E#Value] =
    (x: E#Value, y: E#Value) => w.value(x.id).compare(w.value(y.id))
}
