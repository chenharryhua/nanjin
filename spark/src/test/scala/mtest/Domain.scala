package mtest

import java.time.{Instant, LocalDate, LocalDateTime}

import cats.Show
import io.circe.generic.JsonCodec
import cats.implicits._
import frameless.Injection

final case class EmbeddedForTaskSerializable(f: Int, g: LocalDateTime)

final case class ForTaskSerializable(
  a: Int,
  b: String,
  c: LocalDate,
  d: LocalDateTime,
  e: EmbeddedForTaskSerializable)

case class trip_record(
  VendorID: Int,
  tpep_pickup_datetime: String,
  tpep_dropoff_datetime: String,
  passenger_count: Int,
  trip_distance: Double,
  pickup_longitude: Double,
  pickup_latitude: Double,
  RateCodeID: Int,
  store_and_fwd_flag: String,
  dropoff_longitude: Double,
  dropoff_latitude: Double,
  payment_type: Int,
  fare_amount: Double,
  extra: Double,
  mta_tax: Double,
  improvement_surcharge: Double,
  tip_amount: Double,
  tolls_amount: Double,
  total_amount: Double)

object trip_record {
  implicit val showtrip_record: Show[trip_record] = cats.derived.semi.show[trip_record]
}

@JsonCodec case class Payment(
  id: String,
  time: String,
  amount: BigDecimal,
  currency: String,
  creditCardId: String,
  merchantId: Long)

sealed trait Colorish

object Colorish {

  implicit val colorInjection: Injection[Colorish, String] = new Injection[Colorish, String] {

    override def apply(a: Colorish): String = a match {
      case Red   => "red"
      case Blue  => "blue"
      case Green => "green"
    }

    override def invert(b: String): Colorish = b match {
      case "red"   => Red
      case "blue"  => Blue
      case "green" => Green
    }
  }
  case object Red extends Colorish
  case object Green extends Colorish
  case object Blue extends Colorish
}

final case class Pencil(name: String, color: Colorish)

case class FirstStream(name: String, age: Int)
case class SecondStream(name: String, score: Int)
