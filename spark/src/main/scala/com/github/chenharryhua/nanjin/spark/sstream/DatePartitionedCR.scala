package com.github.chenharryhua.nanjin.spark.sstream

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.kafka.OptionalKV
import io.scalaland.chimney.dsl._
import java.time.ZoneId

final private[spark] case class DatePartitionedCR[K, V](
  Year: String,
  Month: String,
  Day: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V],
  topic: String,
  timestampType: Int
)

private[spark] object DatePartitionedCR {

  def apply[K, V](zoneId: ZoneId)(kv: OptionalKV[K, V]): DatePartitionedCR[K, V] = {
    val time = NJTimestamp(kv.timestamp)
    kv.into[DatePartitionedCR[K, V]]
      .withFieldConst(_.Year, time.yearStr(zoneId))
      .withFieldConst(_.Month, time.monthStr(zoneId))
      .withFieldConst(_.Day, time.dayStr(zoneId))
      .transform
  }
}
