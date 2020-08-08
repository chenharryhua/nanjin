package com.github.chenharryhua.nanjin.spark

import java.time.ZoneId

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.messages.kafka.OptionalKV
import monocle.macros.Lenses

final private[spark] case class NJRepartition(value: Int) extends AnyVal

final private[spark] case class NJPath(value: String) extends AnyVal {

  def append(sub: String): NJPath = {
    val s = if (sub.startsWith("/")) sub.tail else sub
    val v = if (value.endsWith("/")) value.dropRight(1) else value
    NJPath(s"$v/$s")
  }
}

@Lenses final private[spark] case class NJShowDataset(rowNum: Int, isTruncate: Boolean)

final private[spark] case class NJCheckpoint(value: String) {
  require(!value.contains(" ") && value.nonEmpty, "should not empty or contains empty string")

  def append(sub: String): NJCheckpoint = {
    val s = if (sub.startsWith("/")) sub.tail else sub
    val v = if (value.endsWith("/")) value.dropRight(1) else value
    NJCheckpoint(s"$v/$s")
  }
}

final private[spark] case class NJFailOnDataLoss(value: Boolean) extends AnyVal

final private[spark] case class DatePartitionedCR[K, V](
  Year: String,
  Month: String,
  Day: String,
  partition: Int,
  offset: Long,
  timestamp: Long,
  key: Option[K],
  value: Option[V])

object DatePartitionedCR {

  def apply[K, V](zoneId: ZoneId)(kv: OptionalKV[K, V]): DatePartitionedCR[K, V] = {
    val time = NJTimestamp(kv.timestamp)
    DatePartitionedCR(
      time.yearStr(zoneId),
      time.monthStr(zoneId),
      time.dayStr(zoneId),
      kv.partition,
      kv.offset,
      kv.timestamp,
      kv.key,
      kv.value)
  }
}
