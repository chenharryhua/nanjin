package com.github.chenharryhua.nanjin.spark

import cats.Order
import com.github.chenharryhua.nanjin.messages.kafka.codec.KJson
import frameless.{Injection, SQLDate, SQLTimestamp}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Json}
import io.circe.jawn.{decode, parse}
import io.circe.syntax.*
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import shapeless.Witness

import java.sql.{Date, Timestamp}
import java.time.LocalDate

private[spark] trait InjectionInstances extends Serializable {

  // date-time
  implicit val oderSQLDate: Order[SQLDate] =
    (x: SQLDate, y: SQLDate) => x.days.compareTo(y.days)

  implicit val orderSQLTimestamp: Order[SQLTimestamp] =
    (x: SQLTimestamp, y: SQLTimestamp) => x.us.compareTo(y.us)

  implicit val localDateInjection: Injection[LocalDate, Date] =
    new Injection[LocalDate, Date] {
      override def apply(a: LocalDate): Date  = Date.valueOf(a)
      override def invert(b: Date): LocalDate = b.toLocalDate
    }

  implicit val dateInjection: Injection[Date, SQLDate] = new Injection[Date, SQLDate] {
    override def apply(a: Date): SQLDate  = SQLDate(DateTimeUtils.fromJavaDate(a))
    override def invert(b: SQLDate): Date = DateTimeUtils.toJavaDate(b.days)
  }

  implicit val timestampInjection: Injection[Timestamp, SQLTimestamp] =
    new Injection[Timestamp, SQLTimestamp] {

      override def apply(a: Timestamp): SQLTimestamp =
        SQLTimestamp(DateTimeUtils.fromJavaTimestamp(a))

      override def invert(b: SQLTimestamp): Timestamp =
        DateTimeUtils.toJavaTimestamp(b.us)
    }

  // enums
  implicit def enumToStringInjection[E <: Enumeration](implicit
    w: Witness.Aux[E]): Injection[E#Value, String] =
    Injection(_.toString, w.value.withName(_))

  // circe/json
  implicit def kjsonInjection[A: JsonEncoder: JsonDecoder]: Injection[KJson[A], String] =
    new Injection[KJson[A], String] {
      override def apply(a: KJson[A]): String = a.asJson.noSpaces

      override def invert(b: String): KJson[A] = decode[KJson[A]](b) match {
        case Right(r) => r
        case Left(ex) => throw ex
      }
    }

  implicit val circeJsonInjection: Injection[Json, String] = new Injection[Json, String] {
    override def apply(a: Json): String = a.noSpaces

    override def invert(b: String): Json = parse(b) match {
      case Right(r) => r
      case Left(ex) => throw ex
    }
  }
}
