package com.github.chenharryhua.nanjin.spark

import cats.Order
import com.github.chenharryhua.nanjin.messages.kafka.codec.{KJson, KPB}
import frameless.{Injection, SQLDate, SQLTimestamp}
import io.circe.Decoder.Result
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Codec, HCursor, Json, Decoder => JsonDecoder, Encoder => JsonEncoder}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}
import shapeless.Witness

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

private[spark] trait InjectionInstances extends Serializable {

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

  implicit val instantInjection: Injection[Instant, Timestamp] =
    new Injection[Instant, Timestamp] {
      override def apply(a: Instant): Timestamp  = Timestamp.from(a)
      override def invert(b: Timestamp): Instant = b.toInstant
    }

  implicit val timestampInjection: Injection[Timestamp, SQLTimestamp] =
    new Injection[Timestamp, SQLTimestamp] {

      override def apply(a: Timestamp): SQLTimestamp =
        SQLTimestamp(DateTimeUtils.fromJavaTimestamp(a))

      override def invert(b: SQLTimestamp): Timestamp =
        DateTimeUtils.toJavaTimestamp(b.us)
    }

  implicit def enumToStringInjection[E <: Enumeration](implicit
    w: Witness.Aux[E]): Injection[E#Value, String] =
    Injection(_.toString, x => w.value.withName(x))

  implicit def enumCirceEncoder[E <: Enumeration](implicit
    w: Witness.Aux[E]): JsonEncoder[E#Value] =
    JsonEncoder.encodeEnumeration(w.value)

  implicit def enumCirceDecoder[E <: Enumeration](implicit
    w: Witness.Aux[E]): JsonDecoder[E#Value] =
    JsonDecoder.decodeEnumeration(w.value)

  implicit def kjsonInjection[A: JsonEncoder: JsonDecoder]: Injection[KJson[A], String] =
    new Injection[KJson[A], String] {
      override def apply(a: KJson[A]): String = a.asJson.noSpaces

      override def invert(b: String): KJson[A] = decode[KJson[A]](b) match {
        case Right(r) => r
        case Left(ex) => throw ex
      }
    }

  implicit def kpbInjection[A <: GeneratedMessage](implicit
    ev: GeneratedMessageCompanion[A]): Injection[KPB[A], Array[Byte]] =
    new Injection[KPB[A], Array[Byte]] {
      override def apply(a: KPB[A]): Array[Byte]  = a.value.toByteArray
      override def invert(b: Array[Byte]): KPB[A] = KPB(ev.parseFrom(b))
    }

  implicit val timestampCirceCodec: Codec[Timestamp] = new Codec[Timestamp] {
    import io.circe.syntax._
    override def apply(a: Timestamp): Json = a.toInstant.asJson

    override def apply(c: HCursor): Result[Timestamp] =
      JsonDecoder[Instant].apply(c).map(Timestamp.from)
  }

  implicit val dateCirceCodec: Codec[Date] = new Codec[Date] {
    import io.circe.syntax._
    override def apply(a: Date): Json = a.toLocalDate.asJson

    override def apply(c: HCursor): Result[Date] =
      JsonDecoder[LocalDate].apply(c).map(Date.valueOf)
  }

  implicit def orderScalaEnum[E <: Enumeration](implicit
    w: shapeless.Witness.Aux[E]): Order[E#Value] =
    (x: E#Value, y: E#Value) => w.value(x.id).compare(w.value(y.id))
}
