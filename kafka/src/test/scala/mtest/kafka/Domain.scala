package mtest.kafka

import cats.Show

case class Payment(
  id: String,
  time: String,
  amount: BigDecimal,
  currency: String,
  creditCardId: String,
  merchantId: Long)

object Payment {
  implicit val showPayment: Show[Payment] = cats.derived.semiauto.show[Payment]
}

case class lenses_record_key(serial_number: String)

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

object Materials extends Enumeration {
  type Materials = Value
  val Wood, Steel, Stone = Value
  implicit val show: Show[Value] = _.toString
}

sealed trait Colorish

object Colorish {

  implicit val showColorish: Show[Colorish] = (t: Colorish) => t.toString
  case object Red extends Colorish
  case object Green extends Colorish
  case object Blue extends Colorish
}
