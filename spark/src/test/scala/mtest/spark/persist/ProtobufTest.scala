package mtest.spark.persist

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, KPB}
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.persist.loaders
import mtest.spark.pb.test.Whale
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object ProtobufTestData {
  val data  = List.fill(10)(KPB(Whale("a", Random.nextInt())))
  val rdd   = sparkSession.sparkContext.parallelize(data)
  val codec = AvroCodec[KPB[Whale]]
  // val ate   = AvroTypedEncoder[KPB[Whale]](codec)
}

@DoNotDiscover
class ProtobufTest extends AnyFunSuite {
  import ProtobufTestData._
  test("protobuf - single file") {
    val path = "./data/test/spark/persist/protobuf/single.whale.pb"
    rdd.save[IO](codec).protobuf(path).file.run(blocker).unsafeRunSync()
    val res = loaders.rdd.protobuf[KPB[Whale]](path).collect().toSet
    assert(data.toSet == res)
  }

  test("protobuf - multi files") {
    val path = "./data/test/spark/persist/protobuf/multi.whale.pb/"
    rdd.repartition(2).save[IO](codec).protobuf(path).folder.run(blocker).unsafeRunSync()
    val res = loaders.rdd.protobuf[Whale](path).collect().toSet
    assert(data.map(_.value).toSet == res)
  }
}
