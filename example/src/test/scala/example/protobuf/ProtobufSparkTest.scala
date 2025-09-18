package example.protobuf

import cats.effect.IO
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import example.sparkSession
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object ProtobufData {
  val lions: List[Lion] =
    (1 to 10).map(_ => Lion("Melbourne Zoo", Random.nextInt())).toList

  val herd: List[Lion] = (1 to 10000).map(Lion("ChengDu Zoo", _)).toList

//  import sparkSession.implicits.*
//  val rdd = sparkSession.createDataset(lions).rdd
}

class ProtobufSparkTest extends AnyFunSuite {
//  import ProtobufData.*
  val hdp = sparkSession.hadoop[IO]

  // val topic = sparKafka.topic(TopicDef[Int, Lion](TopicName("test")))

//  test("protobuf gzip") {
//    val path = NJPath("./data/test/protobuf/uncompress.proto.gz")
//    saveRDD.protobuf(rdd, path, NJCompression.Gzip)
//    val t1 = loaders.rdd.protobuf[KPB[Lion]](path, sparkSession).collect().toSet
//    assert(t1 == ProtobufData.lions.toSet)
//  }
}
