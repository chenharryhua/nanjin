package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.implicits._

case class Payment(
  id: String,
  time: String,
  amount: BigDecimal,
  currency: String,
  creditCardId: String,
  merchantId: Long)

object Payment {
  implicit val showPayment: Show[Payment] = cats.derived.semi.show[Payment]
}

case class lenses_record_key(serial_number: String)
case class lenses_record(
  date: String,
  serial_number: String,
  model: String,
  capacity_bytes: Option[Long],
  failure: Option[Long],
  smart_1_normalized: Option[Long],
  smart_1_raw: Option[Long],
  smart_2_normalized: Option[Long],
  smart_2_raw: Option[Long],
  smart_3_normalized: Option[Long],
  smart_3_raw: Option[Long],
  smart_4_normalized: Option[Long],
  smart_4_raw: Option[Long],
  smart_5_normalized: Option[Long],
  smart_5_raw: Option[Long],
  smart_7_normalized: Option[Long],
  smart_7_raw: Option[Long],
  smart_8_normalized: Option[Long],
  smart_8_raw: Option[Long],
  smart_9_normalized: Option[Long],
  smart_9_raw: Option[Long],
  smart_10_normalized: Option[Long],
  smart_10_raw: Option[Long],
  smart_11_normalized: Option[Long],
  smart_11_raw: Option[Long],
  smart_12_normalized: Option[Long],
  smart_12_raw: Option[Long],
  smart_13_normalized: Option[Long],
  smart_13_raw: Option[Long],
  smart_15_normalized: Option[Long],
  smart_15_raw: Option[Long],
  smart_22_normalized: Option[Long],
  smart_22_raw: Option[Long],
  smart_183_normalized: Option[Long],
  smart_183_raw: Option[Long],
  smart_184_normalized: Option[Long],
  smart_184_raw: Option[Long],
  smart_187_normalized: Option[Long],
  smart_187_raw: Option[Long],
  smart_188_normalized: Option[Long],
  smart_188_raw: Option[Long],
  smart_189_normalized: Option[Long],
  smart_189_raw: Option[Long],
  smart_190_normalized: Option[Long],
  smart_190_raw: Option[Long],
  smart_191_normalized: Option[Long],
  smart_191_raw: Option[Long],
  smart_192_normalized: Option[Long],
  smart_192_raw: Option[Long],
  smart_193_normalized: Option[Long],
  smart_193_raw: Option[Long],
  smart_194_normalized: Option[Long],
  smart_194_raw: Option[Long],
  smart_195_normalized: Option[Long],
  smart_195_raw: Option[Long],
  smart_196_normalized: Option[Long],
  smart_196_raw: Option[Long],
  smart_197_normalized: Option[Long],
  smart_197_raw: Option[Long],
  smart_198_normalized: Option[Long],
  smart_198_raw: Option[Long],
  smart_199_normalized: Option[Long],
  smart_199_raw: Option[Long],
  smart_200_normalized: Option[Long],
  smart_200_raw: Option[Long],
  smart_201_normalized: Option[Long],
  smart_201_raw: Option[Long],
  smart_220_normalized: Option[Long],
  smart_220_raw: Option[Long],
  smart_222_normalized: Option[Long],
  smart_222_raw: Option[Long],
  smart_223_normalized: Option[Long],
  smart_223_raw: Option[Long],
  smart_224_normalized: Option[Long],
  smart_224_raw: Option[Long],
  smart_225_normalized: Option[Long],
  smart_225_raw: Option[Long],
  smart_226_normalized: Option[Long],
  smart_226_raw: Option[Long],
  smart_240_normalized: Option[Long],
  smart_240_raw: Option[Long],
  smart_241_normalized: Option[Long],
  smart_241_raw: Option[Long],
  smart_242_normalized: Option[Long],
  smart_242_raw: Option[Long],
  smart_250_normalized: Option[Long],
  smart_250_raw: Option[Long],
  smart_251_normalized: Option[Long],
  smart_251_raw: Option[Long],
  smart_252_normalized: Option[Long],
  smart_252_raw: Option[Long],
  smart_254_normalized: Option[Long],
  smart_254_raw: Option[Long],
  smart_255_normalized: Option[Long],
  smart_255_raw: Option[Long])

case class reddit_post(
  created_utc: Int,
  ups: Int,
  subreddit_id: String,
  link_id: String,
  name: String,
  score_hidden: Int,
  author_flair_css_class: Option[String],
  author_flair_text: Option[String],
  subreddit: String,
  id: String,
  removal_reason: Option[String],
  gilded: Int,
  downs: Int,
  archived: Boolean,
  author: String,
  score: Int,
  retrieved_on: Int,
  body: String,
  distinguished: Option[String],
  edited: Int,
  controversiality: Boolean,
  parent_id: String)
case class reddit_post_key(subreddit_id: String)

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

case class Key(MMSI: Long)
case class aisClassAPositionReport(
  Type: Int,
  Repeat: Int,
  MMSI: Long,
  Speed: Float,
  Accuracy: Boolean,
  Longitude: Double,
  Latitude: Double,
  location: String,
  Course: Float,
  Heading: Int,
  Second: Int,
  RAIM: Boolean,
  Radio: Long,
  Status: Int,
  Turn: Float,
  Maneuver: Int,
  Timestamp: Long)

object Materials extends Enumeration {
  type Materials = Value
  val Wood, Steel, Stone         = Value
  implicit val show: Show[Value] = _.toString
}

sealed trait Colorish

object Colorish {
  case object Red extends Colorish
  case object Green extends Colorish
  case object Blue extends Colorish
}
