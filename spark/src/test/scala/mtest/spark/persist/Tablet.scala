package mtest.spark.persist

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.generic._
import org.apache.spark.rdd.RDD

import scala.util.Random

final case class Tablet(a: Int, b: Long, c: Float)

object Tablet {
  val codec: AvroCodec[Tablet]          = AvroCodec[Tablet]
  implicit val te: TypedEncoder[Tablet] = shapeless.cachedImplicit
  val ate: AvroTypedEncoder[Tablet]     = AvroTypedEncoder(codec)
  implicit val re: RowEncoder[Tablet]   = shapeless.cachedImplicit
}

object TabletData {

  val data: List[Tablet] =
    List.fill(10000)(Tablet(Random.nextInt(), Random.nextLong(), Random.nextFloat()))

  val rdd: RDD[Tablet] = sparkSession.sparkContext.parallelize(data)

}
