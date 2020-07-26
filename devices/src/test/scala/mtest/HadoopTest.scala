package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.devices.NJHadoop
import fs2.Stream
import fs2.io.readInputStream
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

object HadoopTestData {

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
                                                        |    },
                                                        |    {
                                                        |      "name": "id",
                                                        |      "type": "int"
                                                        |    }
                                                        |  ]
                                                        |}
                                                        |""".stripMargin)

  val youngPanda = new GenericData.Record(pandaSchema)
  youngPanda.put("name", "zhouzhou")
  youngPanda.put("age", 8)
  youngPanda.put("id", Random.nextInt())

  val prettyPanda = new GenericData.Record(pandaSchema)
  prettyPanda.put("name", "fanfan")
  prettyPanda.put("age", 8)
  prettyPanda.put("id", Random.nextInt())

  val pandas: List[GenericRecord] = List(youngPanda, prettyPanda)
}

class HadoopTest extends AnyFunSuite {
  val hdp = new NJHadoop[IO](new Configuration(), blocker)

  test("hadoop text write/read identity") {
    val pathStr    = "./data/test/devices/greeting.txt"
    val testString = s"hello hadoop ${Random.nextInt()}"
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

  test("parquet write/read indentity") {
    import HadoopTestData._
    val pathStr = "./data/test/devices/panda.parquet"
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.parquetSink(pathStr, pandaSchema)).compile.drain >>
      hdp.parquetSource(pathStr,pandaSchema).compile.toList
    assert(action.unsafeRunSync == pandas)
  }

  test("avro write/read indentity") {
    import HadoopTestData._
    val pathStr = "./data/test/devices/panda.avro"
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.avroSink(pathStr, pandaSchema)).compile.drain >>
      hdp.avroSource(pathStr,pandaSchema).compile.toList
    assert(action.unsafeRunSync == pandas)
  }
}
