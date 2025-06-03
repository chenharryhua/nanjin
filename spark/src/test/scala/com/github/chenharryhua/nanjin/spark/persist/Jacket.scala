package com.github.chenharryhua.nanjin.spark.persist

import com.github.chenharryhua.nanjin.common.transformers.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import io.circe.generic.JsonCodec
import io.circe.{Codec, Json}
import io.circe.generic.auto.*
import io.circe.jawn.parse
import mtest.spark.*
import org.apache.spark.rdd.RDD

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.time.temporal.ChronoUnit
import scala.util.Random

object Pocket extends Enumeration {
  val R, L = Value
}
@JsonCodec
final case class Neck(d: Date, t: Timestamp, j: Json)
@JsonCodec
final case class Jacket(a: Int, p: Pocket.Value, neck: KJson[Neck])

object Jacket {
  val avroCodec: AvroCodec[Jacket] = AvroCodec[Jacket]
  val circe: Codec[Jacket] = io.circe.generic.semiauto.deriveCodec[Jacket]
}

object JacketData {
  implicit val porder: Ordering[Pocket.Value] = Ordering.by(_.id)

  val expected: List[Jacket] =
    List
      .fill(10)(
        Jacket(
          a = Random.nextInt(),
          p = if (Random.nextBoolean()) Pocket.L else Pocket.R,
          neck = KJson(Neck(
            d = Date.valueOf(LocalDate.now()),
            t = Timestamp.from(Instant.now.truncatedTo(ChronoUnit.MILLIS)),
            j =
              parse(s""" {"jsonFloat":"${Random.nextFloat()}","jsonInt":${Random.nextInt()}} """).toOption.get
          ))
        ))
      .sortBy(_.p)
  val rdd: RDD[Jacket] = sparkSession.sparkContext.parallelize(expected)
}
