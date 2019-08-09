package com.github.chenharryhua.nanjin.sparkafka

import cats.Show
import cats.implicits._
import io.circe.generic.JsonCodec
class Domain {}

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
