package mtest.spark

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.NJAvroCodec
import com.github.chenharryhua.nanjin.spark.AvroTypedEncoder
import frameless.cats.implicits.framelessCatsSparkDelayForSync
import org.apache.spark.rdd.RDD
import org.scalatest.funsuite.AnyFunSuite

object AvroTypedEncoderTestData {

  implicit val stringCodec: NJAvroCodec[String] = NJAvroCodec[String]
  implicit val ate: AvroTypedEncoder[String]    = AvroTypedEncoder[String]

  val rdd: RDD[String] = sparkSession.sparkContext.parallelize(List("a", "b", "c"))

}

class AvroTypedEncoderTest extends AnyFunSuite {
  import AvroTypedEncoderTestData._
  ignore("normalize string -- to be done") {
    ate.normalize(rdd).show[IO]().unsafeRunSync()
  }
}
