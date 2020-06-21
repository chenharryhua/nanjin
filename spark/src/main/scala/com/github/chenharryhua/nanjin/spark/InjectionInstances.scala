package com.github.chenharryhua.nanjin.spark

import java.sql.{Date, Timestamp}
import java.time._

import cats.Order
import cats.implicits._
import frameless.{Injection, SQLDate, SQLTimestamp}
import io.scalaland.chimney.Transformer
import monocle.Iso
import org.apache.spark.sql.catalyst.util.DateTimeUtils

private[spark] trait InjectionInstances extends Serializable {

  // monocle iso
  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit val isoJavaSQLTimestamp: Iso[Timestamp, SQLTimestamp] =
    Iso[Timestamp, SQLTimestamp](a => SQLTimestamp(DateTimeUtils.fromJavaTimestamp(a)))(b =>
      DateTimeUtils.toJavaTimestamp(b.us))

  implicit val isoJavaSQLDate: Iso[Date, SQLDate] =
    Iso[Date, SQLDate](a => SQLDate(DateTimeUtils.fromJavaDate(a)))(b =>
      DateTimeUtils.toJavaDate(b.days))

  implicit val isoLocalDate: Iso[LocalDate, SQLDate] =
    Iso[LocalDate, SQLDate](a => SQLDate(a.toEpochDay.toInt))(b =>
      LocalDate.ofEpochDay(b.days.toLong))

  implicit val oderSQLDate: Order[SQLDate] =
    (x: SQLDate, y: SQLDate) => x.days.compareTo(y.days)

  implicit val orderSQLTimestamp: Order[SQLTimestamp] =
    (x: SQLTimestamp, y: SQLTimestamp) => x.us.compareTo(y.us)

  // injection
  implicit def isoInjection[A, B](implicit iso: Iso[A, B]): Injection[A, B] =
    Injection[A, B](iso.get, iso.reverseGet)

  // chimney transformers
  implicit def chimneyTransform[A, B](implicit iso: Iso[A, B]): Transformer[A, B] =
    (src: A) => iso.get(src)

  implicit def chimneyTransformReverse[A, B](implicit iso: Iso[A, B]): Transformer[B, A] =
    (src: B) => iso.reverseGet(src)

}
