package mtest.terminals

import akka.stream.Materializer
import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.syntax.all.*
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

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

  val cfg               = new Configuration()
  val hdp: NJHadoop[IO] = NJHadoop[IO](cfg)

}

class HadoopTest extends AnyFunSuite {
  import HadoopTestData.*

  test("hadoop text write/read identity") {
    val pathStr    = NJPath("./data/test/devices/") / "greeting.txt"
    val testString = s"hello hadoop ${Random.nextInt()}"
    val ts: Stream[IO, Byte] =
      Stream(testString).through(fs2.text.utf8.encode)

    val action = hdp.delete(pathStr) >>
      ts.through(hdp.byteSink(pathStr)).compile.drain >>
      hdp.byteSource(pathStr).through(fs2.text.utf8.decode).compile.toList
    assert(action.unsafeRunSync().mkString == testString)
  }

  test("snappy avro write/read") {
    val pathStr = NJPath("./data/test/devices/panda.snappy.avro")
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.avroSink(pathStr, pandaSchema, CodecFactory.snappyCodec)).compile.drain >>
      hdp.avroSource(pathStr, pandaSchema, 100).compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("deflate(6) avro write/read") {
    val pathStr = NJPath("./data/test/devices/panda.deflate.avro")
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.avroSink(pathStr, pandaSchema, CodecFactory.deflateCodec(6))).compile.drain >>
      hdp.avroSource(pathStr, pandaSchema, 100).compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("uncompressed avro write/read") {
    val pathStr = NJPath("./data/test/devices/panda.uncompressed.avro")
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.avroSink(pathStr, pandaSchema, CodecFactory.nullCodec)).compile.drain >>
      hdp.avroSource(pathStr, pandaSchema, 100).compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("uncompressed avro write/read akka") {
    val pathStr                    = NJPath("./data/test/devices/akka/panda.uncompressed.avro")
    val ts                         = Source(pandas)
    implicit val mat: Materializer = Materializer(akkaSystem)
    val action = hdp.delete(pathStr) >>
      IO.fromFuture(IO(ts.runWith(hdp.akka.avroSink(pathStr, pandaSchema, CodecFactory.nullCodec)))) >>
      IO.fromFuture(IO(hdp.akka.avroSource(pathStr, pandaSchema).runFold(List.empty[GenericRecord])(_.appended(_))))
    assert(action.unsafeRunSync() == pandas)
  }

  test("dataFolders") {
    val pathStr = NJPath("./data/test/devices")
    val folders = hdp.dataFolders(pathStr).unsafeRunSync()
    assert(folders.headOption.exists(_.toUri.getPath.contains("devices")))
  }

  test("hadoop input files") {
    val path = NJPath("data/test/devices")
    hdp.hadoopInputFiles(path).flatMap(_.traverse(x => IO.println(x.toString))).unsafeRunSync()
  }
}
