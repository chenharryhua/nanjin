package com.github.chenharryhua.nanjin.spark

import java.sql.{Date, Timestamp}

import cats.Order
import cats.implicits._
import frameless.{Injection, SQLDate, SQLTimestamp}
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
    w: Witness.Aux[E]): Injection[E#Value, String] = Injection(_.toString, x => w.value.withName(x))
}
