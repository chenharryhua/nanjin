package mtest.spark.persist

import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KJson}
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import io.circe.generic.auto._

import scala.util.Random
final case class Neck(a: Int, b: Int)
final case class Jacket(c: Int, neck: KJson[Neck])

object Jacket {
  implicit val te: TypedEncoder[Jacket] = shapeless.cachedImplicit
  val avroCodec: AvroCodec[Jacket]      = AvroCodec[Jacket]
  val ate: AvroTypedEncoder[Jacket]     = AvroTypedEncoder(avroCodec)
}

object JacketData {

  val expected =
    List.fill(10)(Jacket(Random.nextInt, KJson(Neck(0, 1))))
  val rdd = sparkSession.sparkContext.parallelize(expected)
  val ds  = Jacket.ate.normalize(rdd)
}

object test {
  final class ABC[A](val a: A) extends Serializable

  object ABC {
    def apply[A](a: A) = new ABC(a)
  }
  final class Z(a: Int, b: ABC[Neck])
  val te: TypedEncoder[Z] = shapeless.cachedImplicit

}
