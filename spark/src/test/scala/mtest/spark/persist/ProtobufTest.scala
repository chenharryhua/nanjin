package mtest.spark.persist

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.messages.kafka.codec.KPB
import com.github.chenharryhua.nanjin.spark.injection.*
import com.github.chenharryhua.nanjin.spark.persist.{loaders, RddFileHoarder}
import mtest.pb.spark.Whale
import mtest.spark.*
import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random
object ProtobufTestData {
  val data = List.fill(10)(KPB(Whale("a", Random.nextInt())))
  val rdd  = sparkSession.sparkContext.parallelize(data)
}

@DoNotDiscover
class ProtobufTest extends AnyFunSuite {
  import ProtobufTestData.*
  val saver = new RddFileHoarder[IO, Whale](rdd.map(_.value).repartition(2))
  test("protobuf - single file") {
    val path = "./data/test/spark/persist/protobuf/single.whale.pb"
    saver.protobuf(path).file.errorIfExists.ignoreIfExists.overwrite.sink.compile.drain.unsafeRunSync()
    val res = loaders.rdd.protobuf[KPB[Whale]](path, sparkSession).collect().toSet
    assert(data.toSet == res)
  }

  test("protobuf - multi files") {
    val path = "./data/test/spark/persist/protobuf/multi.whale.pb/"
    saver.protobuf(path).folder.run.unsafeRunSync()
    val res = loaders.rdd.protobuf[Whale](path, sparkSession).collect().toSet
    assert(data.map(_.value).toSet == res)
  }
}
