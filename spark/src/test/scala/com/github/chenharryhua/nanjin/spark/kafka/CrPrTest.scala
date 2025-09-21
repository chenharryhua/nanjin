package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.{ NJConsumerRecord}
import com.github.chenharryhua.nanjin.spark.persist.{Rooster, RoosterData}
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import io.scalaland.chimney.dsl.*
import mtest.spark.kafka.sparKafka
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroFor
final case class RoosterLike(c: BigDecimal, d: BigDecimal)

object RoosterLike {
  def apply(r: Rooster): RoosterLike = r.transformInto[RoosterLike]
}

final case class RoosterLike2(c: BigDecimal, d: BigDecimal, e: Int)

object RoosterLike2 {
  def apply(r: Rooster): Option[RoosterLike2] = r.e.map(x => RoosterLike2(r.c, r.d, x))
}

class CrPrTest extends AnyFunSuite {
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP
  implicit val te1: TypedEncoder[Rooster] = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[RoosterLike] = shapeless.cachedImplicit
  implicit val te3: TypedEncoder[RoosterLike2] = shapeless.cachedImplicit

  val rooster = AvroTopic[Long, Rooster](TopicName("rooster"))(AvroFor[Long], AvroFor(Rooster.avroCodec))
 // val roosterATE = SchematizedEncoder(rooster.pair)

  val roosterLike =
    AvroTopic[Long, RoosterLike](TopicName("roosterLike"))(AvroFor[Long], AvroFor[RoosterLike])

  val roosterLike2 =
    AvroTopic[Long, RoosterLike2](TopicName("roosterLike2"))(AvroFor[Long], AvroFor[RoosterLike2])

  val crRdd: CrRdd[Long, Rooster] = sparKafka
    .topic(rooster)
    .crRdd(RoosterData.rdd.zipWithIndex().map { case (r, i) =>
      NJConsumerRecord(
        topic = "rooster",
        partition = 0,
        offset = i,
        timestamp = Instant.now.getEpochSecond * 1000 + i,
        timestampType = 0,
        serializedKeySize = None,
        serializedValueSize = None,
        key = Some(i),
        value = Some(r),
        headers = Nil,
        leaderEpoch = None
      )
    })

  val expectSchema = StructType(
    List(
      StructField("partition", IntegerType, false),
      StructField("offset", LongType, false),
      StructField("timestamp", LongType, false),
      StructField("key", LongType, true),
      StructField(
        "value",
        StructType(
          List(StructField("c", DecimalType(8, 2), false), StructField("d", DecimalType(8, 2), false))),
        true),
      StructField("topic", StringType, false),
      StructField("timestampType", IntegerType, false)
    ))

  val prRdd: PrRdd[Long, Rooster] = crRdd.prRdd.partitionOf(0)
  val topic = roosterLike
  val ack = topic.pair.key.schema
  val acv = topic.pair.key.schema

  test("time range") {
    val dr =
      DateTimeRange(sydneyTime)
        .withStartTime(Instant.now.minusSeconds(50))
        .withEndTime(Instant.now().plusSeconds(10))
    assert(crRdd.timeRange(dr).rdd.collect().length == 4)
    assert(crRdd.prRdd.partitionOf(0).timeRange(dr).rdd.collect().length == 4)
    assert(crRdd.prRdd.timeRange(dr).rdd.collect().size == 4)
  }

  test("offset range") {
    assert(crRdd.offsetRange(0, 2).rdd.collect().size == 3)
    assert(crRdd.prRdd.offsetRange(0, 2).rdd.collect().size == 3)
  }
  test("replicate") {
    assert(prRdd.replicate(3).rdd.count() == 12)
  }

}
