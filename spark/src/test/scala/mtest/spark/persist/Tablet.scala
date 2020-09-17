package mtest.spark.persist

import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import kantan.csv.RowEncoder
import kantan.csv.generic._
import org.apache.spark.rdd.RDD

final case class Tablet(a: Int, b: Long, c: Float)

object Tablet {
  val codec: AvroCodec[Tablet]          = AvroCodec[Tablet]
  implicit val te: TypedEncoder[Tablet] = shapeless.cachedImplicit
  val ate: AvroTypedEncoder[Tablet]     = AvroTypedEncoder(codec)
  implicit val re: RowEncoder[Tablet]   = shapeless.cachedImplicit
}

object TabletData {

  val data = List(
    Tablet(1, 2, 0.1f),
    Tablet(2, 3, 0.2f),
    Tablet(3, 4, 0.3f)
  )

  val rdd: RDD[Tablet] = sparkSession.sparkContext.parallelize(data)

}
