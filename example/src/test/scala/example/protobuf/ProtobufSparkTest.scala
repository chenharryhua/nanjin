package example.protobuf

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, JsonTopic, ProtobufTopic}
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import example.sparkSession
import io.circe.generic.JsonCodec
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
@JsonCodec
final case class JsonLion(name:String, age:Int)
final case class AvroLion(name:String, age:Int)
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
  test("topic") {
    import eu.timepit.refined.auto.*
    val a1 = AvroTopic[Int, AvroLion](TopicName("a"))
    val a2 = JsonTopic[Int, JsonLion](TopicName("a"))
    val a3 = ProtobufTopic[Int, Lion](TopicName("a"))
    println(a1.pair.consumerFormat.codec.schema)
    println(a2.pair.value.jsonSchema)
    println(a3.pair.value.descriptor)
  }
}
