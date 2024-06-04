package com.github.chenharryhua.nanjin.spark.kafka

import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{Rooster, RoosterData}
import eu.timepit.refined.auto.*
import frameless.TypedEncoder
import io.scalaland.chimney.dsl.*
import mtest.spark.kafka.{ctx, sparKafka}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.math.BigDecimal.RoundingMode
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
  implicit val te1: TypedEncoder[Rooster]                  = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[RoosterLike]              = shapeless.cachedImplicit
  implicit val te3: TypedEncoder[RoosterLike2]             = shapeless.cachedImplicit

  val rooster    = TopicDef[Long, Rooster](TopicName("rooster"), Rooster.avroCodec)
  val roosterATE = AvroTypedEncoder(rooster)

  val roosterLike =
    TopicDef[Long, RoosterLike](TopicName("roosterLike"), NJAvroCodec[RoosterLike])

  val roosterLike2 =
    TopicDef[Long, RoosterLike2](TopicName("roosterLike2"), NJAvroCodec[RoosterLike2])

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
    .normalize

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
  val topic                       = ctx.topic(roosterLike)
  val ack                         = topic.topicDef.rawSerdes.key.avroCodec
  val acv                         = topic.topicDef.rawSerdes.key.avroCodec

  test("time range") {
    val dr =
      NJDateTimeRange(sydneyTime)
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
