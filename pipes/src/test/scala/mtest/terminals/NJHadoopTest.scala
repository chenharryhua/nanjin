package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import eu.timepit.refined.auto.*
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

  val pandaSet: Set[GenericRecord] = pandas.toSet

  val cfg               = new Configuration()
  val hdp: NJHadoop[IO] = NJHadoop[IO](cfg)
}

class NJHadoopTest extends AnyFunSuite {
  import HadoopTestData.*

  test("dataFolders") {
    val pathStr = NJPath("./data/test/devices")
    val folders = hdp.dataFolders(pathStr).unsafeRunSync()
    assert(folders.headOption.exists(_.pathStr.contains("devices")))
  }

  test("hadoop input files") {
    val path = NJPath("data/test/devices")
    hdp.filesSortByName(path).flatMap(_.traverse(x => IO.println(x.toString))).unsafeRunSync()
  }
}
