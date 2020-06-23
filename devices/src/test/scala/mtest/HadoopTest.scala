package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.devices.NJHadoop
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import fs2.io.readInputStream
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object HadoopTestData {
  final case class Panda(name: String, age: Int)

  val pandaSchema: Schema = (new Schema.Parser).parse("""
                                                        |{
                                                        |  "type": "record",
                                                        |  "name": "Panda",
                                                        |  "namespace": "mtest.HadoopTestData",
                                                        |  "fields": [
                                                        |    {
                                                        |      "name": "name",
                                                        |      "type": "string"
                                                        |    },
                                                        |    {
                                                        |      "name": "age",
                                                        |      "type": "int"
                                                        |    }
                                                        |  ]
                                                        |}
                                                        |""".stripMargin)

  val pandas: List[GenericRecord] = List()
}

class HadoopTest extends AnyFunSuite {
  val hdp = new NJHadoop[IO](new Configuration(), blocker)

  test("hadoop text write/read identity") {
    val pathStr    = "./data/devices/hadoop/test.txt"
    val testString = s"save string to hadoop ${Random.nextInt()}"
    val ts: Stream[IO, Byte] =
      Stream(testString).through(fs2.text.utf8Encode)

    val action = hdp.delete(pathStr) >>
      ts.through(hdp.byteSink(pathStr)).compile.drain >>
      hdp
        .inputStream(pathStr)
        .flatMap(is => readInputStream(IO(is), 64, blocker).through(fs2.text.utf8Decode))
        .compile
        .toList
    assert(action.unsafeRunSync.head == testString)
  }

//  test("parquet write/read indentity") {
//    import HadoopTestData._
//    val pathStr = "./data/devices/parquet/test.parquet"
//    val ts      = Stream.emits(pandas).covary[IO]
//    val action = hdp.delete(pathStr) >>
//      ts.through(hdp.parquetSink(pathStr, pandaSchema)).compile.drain >>
//      hdp.parquetSource[Panda](pathStr).compile.toList
//    assert(action.unsafeRunSync == pandas)
//  }
//
//  test("avro write/read indentity") {
//    import HadoopTestData._
//    val pathStr = "./data/devices/avro/test.avro"
//    val ts      = Stream.emits(pandas).covary[IO]
//    val action = hdp.delete(pathStr) >>
//      ts.through(hdp.avroSink[Panda](pathStr)).compile.drain >>
//      hdp.avroSource[Panda](pathStr).compile.toList
//    assert(action.unsafeRunSync == pandas)
//  }
}
