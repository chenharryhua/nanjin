package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.persist.{Rooster, RoosterData}
import frameless.TypedEncoder
import io.scalaland.chimney.dsl.*
import mtest.spark.kafka.{ctx, sparKafka}
import org.apache.spark.sql.types.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.Instant
import scala.math.BigDecimal
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
  val roosterATE = NJConsumerRecord.ate(rooster)

  val roosterLike =
    TopicDef[Long, RoosterLike](TopicName("roosterLike"), NJAvroCodec[RoosterLike])

  val roosterLike2 =
    TopicDef[Long, RoosterLike2](TopicName("roosterLike2"), NJAvroCodec[RoosterLike2])

  val crRdd: CrRdd[IO, Long, Rooster] = sparKafka
    .topic(rooster)
    .crRdd(RoosterData.rdd.zipWithIndex().map { case (r, i) =>
      NJConsumerRecord(0, i, Instant.now.getEpochSecond * 1000 + i, Some(i), Some(r), "rooster", 0)
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
        StructType(List(StructField("c", DecimalType(8, 2), false), StructField("d", DecimalType(8, 2), false))),
        true),
      StructField("topic", StringType, false),
      StructField("timestampType", IntegerType, false)
    ))

  val crDS: CrDS[IO, Long, Rooster]   = crRdd.crDS.partitionOf(0)
  val prRdd: PrRdd[IO, Long, Rooster] = crRdd.prRdd.partitionOf(0)
  val topic                           = roosterLike.in(ctx)
  val ack                             = topic.topicDef.rawSerdes.keySerde.avroCodec
  val acv                             = topic.topicDef.rawSerdes.keySerde.avroCodec

  test("bimap") {
    val cr = crDS
      .repartition(1)
      .crRdd
      .bimap(identity, RoosterLike(_))(ack, NJAvroCodec[RoosterLike])
      .rdd
      .collect()
      .flatMap(_.value)
      .toSet

    val ds = crDS.bimap(identity, RoosterLike(_))(ack, NJAvroCodec[RoosterLike]).dataset
    val d  = ds.collect().flatMap(_.value).toSet

    crRdd.rdd.take(3).foreach(println)
    assert(ds.schema == expectSchema)
    assert(cr == d)

  }

  test("map") {
    val cr =
      crRdd
        .map(x => x.newValue(x.value.map(RoosterLike(_))))(ack, NJAvroCodec[RoosterLike])
        .rdd
        .collect()
        .flatMap(_.value)
        .toSet

    val ds = crDS.map(_.bimap(identity, RoosterLike(_)))(ack, NJAvroCodec[RoosterLike]).dataset
    val d  = ds.collect().flatMap(_.value).toSet
    assert(ds.schema == expectSchema)
    assert(cr == d)
  }

  test("flatMap") {
    val cr = crRdd.flatMap { x =>
      x.value.flatMap(RoosterLike2(_)).map(y => x.newValue(Some(y)).newKey(x.key))
    }(ack, NJAvroCodec[RoosterLike2]).rdd.collect().flatMap(_.value).toSet

    val d = crDS.flatMap { x =>
      x.value.flatMap(RoosterLike2(_)).map(y => x.newValue(Some(y)))
    }(ack, NJAvroCodec[RoosterLike2]).dataset.collect().flatMap(_.value).toSet

    assert(cr == d)
  }

  test("union") {
    val r = crRdd.union(crRdd)
    val d = crDS.union(crDS)
    assert(r.count.unsafeRunSync() == d.count.unsafeRunSync())
  }

  test("stats") {
    crDS.stats.daily.unsafeRunSync()
    crRdd.stats.daily.unsafeRunSync()
  }

  test("time range") {
    val dr =
      NJDateTimeRange(sydneyTime).withStartTime(Instant.now.minusSeconds(50)).withEndTime(Instant.now().plusSeconds(10))
    assert(crRdd.timeRange(dr).rdd.collect().size == 4)
    assert(crRdd.prRdd.partitionOf(0).timeRange(dr).rdd.collect().size == 4)
    assert(crRdd.crDS.timeRange(dr).dataset.collect().size == 4)
    assert(crRdd.timeRange.rdd.collect().size == 4)
    assert(crRdd.prRdd.timeRange(dr).rdd.collect().size == 4)
    assert(crRdd.crDS.timeRange.dataset.collect().size == 4)
  }

  test("offset range") {
    assert(crRdd.offsetRange(0, 2).rdd.collect().size == 3)
    assert(crRdd.prRdd.offsetRange(0, 2).rdd.collect().size == 3)
    assert(crRdd.crDS.offsetRange(0, 2).dataset.collect().size == 3)
  }
  test("cherry-pick") {
    assert(crRdd.normalize.cherrypick(0, 0).map(_.value) == crDS.normalize.cherrypick(0, 0).map(_.value))
  }
  test("replicate") {
    assert(crRdd.replicate(3).rdd.count() == 12)
    assert(prRdd.replicate(3).rdd.count() == 12)
    assert(crDS.replicate(3).dataset.count() == 12)
  }
}
