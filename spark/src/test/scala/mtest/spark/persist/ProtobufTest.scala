package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KPB}
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark._
import mtest.spark.pb.test.Whale
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object ProtobufTestData {
  val data  = List.fill(10)(KPB(Whale("a", Random.nextInt())))
  val rdd   = sparkSession.sparkContext.parallelize(data)
  val codec = AvroCodec[KPB[Whale]]
}

class ProtobufTest extends AnyFunSuite {
  import ProtobufTestData._
  test("protobuf") {
    val path = "./data/test/spark/persist/protobuf/whale.pb"
    rdd.save[IO](codec).protobuf(path).run(blocker).unsafeRunSync()
  }
}
