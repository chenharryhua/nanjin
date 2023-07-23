package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.common.time.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.NJDateTimeRange
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerRecord
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.persist.{Rooster, RoosterData}
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

  val crRdd: CrRdd[IO, Long, Rooster] = sparKafka
    .topic(rooster)
    .crRdd(IO(RoosterData.rdd.zipWithIndex().map { case (r, i) =>
      NJConsumerRecord(0, i, Instant.now.getEpochSecond * 1000 + i, Some(i), Some(r), "rooster", 0, Nil)
    }))
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

  val prRdd: PrRdd[IO, Long, Rooster] = crRdd.prRdd.partitionOf(0)
  val topic                           = roosterLike.in(ctx)
  val ack                             = topic.topicDef.rawSerdes.keySerde.avroCodec
  val acv                             = topic.topicDef.rawSerdes.keySerde.avroCodec

  test("time range") {
    val dr =
      NJDateTimeRange(sydneyTime)
        .withStartTime(Instant.now.minusSeconds(50))
        .withEndTime(Instant.now().plusSeconds(10))
    assert(crRdd.timeRange(dr).frdd.map(_.collect().length).unsafeRunSync() == 4)
    assert(crRdd.prRdd.partitionOf(0).timeRange(dr).frdd.map(_.collect().length).unsafeRunSync() == 4)
    assert(crRdd.prRdd.timeRange(dr).frdd.map(_.collect().size).unsafeRunSync() == 4)
  }

  test("offset range") {
    assert(crRdd.offsetRange(0, 2).frdd.map(_.collect().size).unsafeRunSync() == 3)
    assert(crRdd.prRdd.offsetRange(0, 2).frdd.map(_.collect().size).unsafeRunSync() == 3)
  }
  test("replicate") {
    assert(prRdd.replicate(3).frdd.map(_.count()).unsafeRunSync() == 12)
  }
}
