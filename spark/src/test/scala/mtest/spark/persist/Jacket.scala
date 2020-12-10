package mtest.spark.persist

import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.{TypedDataset, TypedEncoder}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.parser.parse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import scala.util.Random
final case class Neck(a: Int, b: Json)
final case class Jacket(c: Int, neck: KJson[Neck])

object Jacket {
  implicit val te: TypedEncoder[Jacket] = shapeless.cachedImplicit
  val avroCodec: AvroCodec[Jacket]      = AvroCodec[Jacket]
  val ate: AvroTypedEncoder[Jacket]     = AvroTypedEncoder(avroCodec)
}

object JacketData {

  val expected: List[Jacket] =
    List.fill(10)(
      Jacket(
        Random.nextInt,
        KJson(
          Neck(
            0,
            parse(
              s""" {"jsonFloat":"${Random.nextFloat()}","jsonInt":${Random
                .nextInt()}} """).right.get))))
  val rdd: RDD[Jacket]    = sparkSession.sparkContext.parallelize(expected)
  val ds: Dataset[Jacket] = Jacket.ate.normalize(rdd).dataset
}
