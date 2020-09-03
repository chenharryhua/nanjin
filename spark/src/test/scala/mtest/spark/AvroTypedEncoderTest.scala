package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.TypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

object AvroTypedEncoderTestData {

  final case class StringWraper(a: String)

  implicit val stringCodec: NJAvroCodec[StringWraper] = NJAvroCodec[StringWraper]
  implicit val te: TypedEncoder[StringWraper]         = shapeless.cachedImplicit
  implicit val ate: AvroTypedEncoder[StringWraper]    = AvroTypedEncoder[StringWraper]

  val rdd: RDD[StringWraper] = sparkSession.sparkContext.parallelize(
    List(StringWraper("a"), StringWraper("b"), StringWraper("c")))

}

class AvroTypedEncoderTest extends AnyFunSuite {
  import AvroTypedEncoderTestData._
  test("normalize string -- to be done") {
    ate.normalize(rdd).show[IO]().unsafeRunSync()
  }
}
