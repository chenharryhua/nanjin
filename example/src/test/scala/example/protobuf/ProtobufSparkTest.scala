package example.protobuf

import cats.effect.IO
import com.github.chenharryhua.nanjin.messages.kafka.codec.KPB
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import eu.timepit.refined.auto.*
import example.{sparKafka, sparkSession}
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object ProtobufData {
  val lions =
    (1 to 10).map(x => (Lion("Melbourne Zoo", Random.nextInt()))).toList

//  import sparkSession.implicits.*
//  val rdd = sparkSession.createDataset(lions).rdd
}

class ProtobufSparkTest extends AnyFunSuite {
  import ProtobufData.*
  val hdp = sparkSession.hadoop[IO]

  val topic = sparKafka.topic[Int, KPB[Lion]]("test")

//  test("protobuf gzip") {
//    val path = NJPath("./data/test/protobuf/uncompress.proto.gz")
//    saveRDD.protobuf(rdd, path, NJCompression.Gzip)
//    val t1 = loaders.rdd.protobuf[KPB[Lion]](path, sparkSession).collect().toSet
//    assert(t1 == ProtobufData.lions.toSet)
//  }
}
