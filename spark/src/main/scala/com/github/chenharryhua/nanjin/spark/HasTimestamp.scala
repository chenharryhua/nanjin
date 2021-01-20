package com.github.chenharryhua.nanjin.spark

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.kafka.NJConsumerRecord

trait HasTimestamp[A] {
  def timestamp(a: A): NJTimestamp
}

object HasTimestamp {

  implicit def njConsumerRecordHasTimestamp[K, V]: HasTimestamp[NJConsumerRecord[K, V]] =
    (a: NJConsumerRecord[K, V]) => NJTimestamp(a.timestamp)
}
