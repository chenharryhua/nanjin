package com.github.chenharryhua.nanjin.spark.kafka

import cats.effect.IO
import cats.syntax.all._
import com.github.chenharryhua.nanjin.datetime.{sydneyTime, NJDateTimeRange}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark._
import frameless.TypedEncoder
import io.scalaland.chimney.dsl._
import mtest.spark.persist.{Rooster, RoosterData}
import mtest.spark.{ctx, sparkSession}
import org.apache.spark.sql.types._
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

class CrDSTest extends AnyFunSuite {
  implicit val roundingMode: BigDecimal.RoundingMode.Value = RoundingMode.HALF_UP
  implicit val te1: TypedEncoder[Rooster]                  = shapeless.cachedImplicit
  implicit val te2: TypedEncoder[RoosterLike]              = shapeless.cachedImplicit
  implicit val te3: TypedEncoder[RoosterLike2]             = shapeless.cachedImplicit

  val sk      = sparkSession.alongWith(ctx)
  val rooster = TopicDef[Long, Rooster](TopicName("rooster"), Rooster.avroCodec)

  val roosterLike =
    TopicDef[Long, RoosterLike](TopicName("roosterLike"), AvroCodec[RoosterLike]).in(ctx)

  val roosterLike2 =
    TopicDef[Long, RoosterLike2](TopicName("roosterLike2"), AvroCodec[RoosterLike2]).in(ctx)

  val crRdd: CrRdd[IO, Long, Rooster] = sk
    .topic(rooster)
    .crRdd(RoosterData.rdd.zipWithIndex.map { case (r, i) =>
      OptionalKV(0, i, Instant.now.getEpochSecond * 1000 + i, Some(i), Some(r), "rooster", 0)
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
          List(
            StructField("c", DecimalType(8, 2), false),
            StructField("d", DecimalType(8, 2), false))),
        true),
      StructField("topic", StringType, false),
      StructField("timestampType", IntegerType, false)
    ))

  val crDS: CrDS[IO, Long, Rooster] = crRdd.crDS

  test("misc") {
    assert(crRdd.keys.collect().size == 4)
    assert(crRdd.values.collect().size == 4)
    assert(crRdd.partitionOf(0).rdd.collect.size == 4)
  }

  test("first") {
    assert(crRdd.first.unsafeRunSync().get.value == RoosterData.data.headOption)

  }

  test("last") {
    assert(crRdd.last.unsafeRunSync().get.value == RoosterData.data.lastOption)
  }

  test("bimap") {
    val r = crRdd.normalize
      .bimap(identity, RoosterLike(_))(roosterLike)
      .rdd
      .collect()
      .flatMap(_.value)
      .toSet

    val ds = crDS.normalize.bimap(identity, RoosterLike(_))(roosterLike).dataset
    val d  = ds.collect().flatMap(_.value).toSet

    assert(ds.schema == expectSchema)
    assert(r == d)
  }

  test("map") {
    val r = crRdd.normalize
      .map(x => x.newValue(x.value.map(RoosterLike(_))))(roosterLike)
      .rdd
      .collect
      .flatMap(_.value)
      .toSet
    val ds = crDS.normalize.map(_.bimap(identity, RoosterLike(_)))(roosterLike).dataset
    val d  = ds.collect.flatMap(_.value).toSet
    assert(ds.schema == expectSchema)
    assert(r == d)
  }

  test("flatMap") {
    val r = crRdd.normalize.flatMap { x =>
      x.value.flatMap(RoosterLike2(_)).map(y => x.newValue(Some(y)).newKey(x.key))
    }(roosterLike2).rdd.collect().flatMap(_.value).toSet

    val d = crDS.normalize.flatMap { x =>
      x.value.flatMap(RoosterLike2(_)).map(y => x.newValue(Some(y)))
    }(roosterLike2).dataset.collect.flatMap(_.value).toSet

    assert(r == d)
  }

  test("filter") {
    val r  = crRdd.filter(_.key.exists(_ == 0)).rdd.collect().flatMap(_.value).headOption
    val ds = crDS.filter(_.key.exists(_ == 0)).dataset
    val d  = ds.collect().flatMap(_.value).headOption
    assert(r == d)
  }
  test("union") {
    val r = crRdd.normalize.union(crRdd)
    val d = crDS.union(crDS)
    assert(r.count.unsafeRunSync() == d.count.unsafeRunSync())
  }

  test("stats") {
    crDS.stats.daily.unsafeRunSync()
    crDS.crRdd.stats.daily.unsafeRunSync()
  }

  test("time range") {
    val dr = NJDateTimeRange(sydneyTime)
      .withStartTime(Instant.now.minusSeconds(50))
      .withEndTime(Instant.now().plusSeconds(10))
    assert(crRdd.timeRange(dr).rdd.collect.size == 4)
    assert(crRdd.prRdd.partitionOf(0).timeRange(dr).rdd.collect.size == 4)
    assert(crRdd.crDS.timeRange(dr).dataset.collect.size == 4)
    assert(crRdd.timeRange.rdd.collect.size == 4)
    assert(crRdd.prRdd.timeRange.rdd.collect.size == 4)
    assert(crRdd.crDS.timeRange.dataset.collect.size == 4)
  }

  test("offset range") {
    assert(crRdd.offsetRange(0, 2).rdd.collect.size == 3)
    assert(crRdd.prRdd.offsetRange(0, 2).rdd.collect.size == 3)
    assert(crRdd.crDS.offsetRange(0, 2).dataset.collect.size == 3)
  }
}
