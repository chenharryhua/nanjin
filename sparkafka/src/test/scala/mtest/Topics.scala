package mtest

import com.github.chenharryhua.nanjin.kafka.TopicDef

class Topics {
  val trip    = TopicDef[Int, trip_record]("nyc_yellow_taxi_trip_data")
  val payment = TopicDef[String, Payment]("cc_payments")
}
