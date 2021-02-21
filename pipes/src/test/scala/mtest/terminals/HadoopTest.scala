package mtest.terminals

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import fs2.Stream
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

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

class HadoopTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  val hdp: NJHadoop[IO] = NJHadoop[IO](new Configuration())

  "Hadoop" - {
    "hadoop text write/read identity" in {
      val pathStr    = "./data/test/devices/greeting.txt"
      val testString = s"hello hadoop ${Random.nextInt()}"
      val ts: Stream[IO, Byte] =
        Stream(testString).through(fs2.text.utf8Encode)

      val action = hdp.delete(pathStr) >>
        ts.through(hdp.byteSink(pathStr)).compile.drain >>
        hdp.byteSource(pathStr).through(fs2.text.utf8Decode).compile.toList
      action.asserting(_.head shouldBe testString)
    }

    "snappy parquet write/read" in {
      import HadoopTestData._
      val pathStr = "./data/test/devices/panda.snappy.parquet"
      val ts      = Stream.emits(pandas).covary[IO]
      val action = hdp.delete(pathStr) >>
        ts.through(hdp.parquetSink(pathStr, pandaSchema, CompressionCodecName.SNAPPY)).compile.drain >>
        hdp.parquetSource(pathStr, pandaSchema).compile.toList
      action.asserting(_ shouldBe pandas)
    }

    "gzip parquet write/read" in {
      import HadoopTestData._
      val pathStr = "./data/test/devices/panda.gzip.parquet"
      val ts      = Stream.emits(pandas).covary[IO]
      val action = hdp.delete(pathStr) >>
        ts.through(hdp.parquetSink(pathStr, pandaSchema, CompressionCodecName.GZIP)).compile.drain >>
        hdp.parquetSource(pathStr, pandaSchema).compile.toList
      action.asserting(_ shouldBe pandas)
    }

    "uncompressed parquet write/read" in {
      import HadoopTestData._
      val pathStr = "./data/test/devices/panda.uncompressed.parquet"
      val ts      = Stream.emits(pandas).covary[IO]
      val action = hdp.delete(pathStr) >>
        ts.through(hdp.parquetSink(pathStr, pandaSchema, CompressionCodecName.UNCOMPRESSED)).compile.drain >>
        hdp.parquetSource(pathStr, pandaSchema).compile.toList
      action.asserting(_ shouldBe pandas)
    }

    "snappy avro write/read" in {
      import HadoopTestData._
      val pathStr = "./data/test/devices/panda.snappy.avro"
      val ts      = Stream.emits(pandas).covary[IO]
      val action = hdp.delete(pathStr) >>
        ts.through(hdp.avroSink(pathStr, pandaSchema, CodecFactory.snappyCodec)).compile.drain >>
        hdp.avroSource(pathStr, pandaSchema).compile.toList
      action.asserting(_ shouldBe pandas)
    }

    "deflate(6) avro write/read" in {
      import HadoopTestData._
      val pathStr = "./data/test/devices/panda.deflate.avro"
      val ts      = Stream.emits(pandas).covary[IO]
      val action = hdp.delete(pathStr) >>
        ts.through(hdp.avroSink(pathStr, pandaSchema, CodecFactory.deflateCodec(6))).compile.drain >>
        hdp.avroSource(pathStr, pandaSchema).compile.toList
      action.asserting(_ shouldBe pandas)
    }

    "uncompressed avro write/read" in {
      import HadoopTestData._
      val pathStr = "./data/test/devices/panda.uncompressed.avro"
      val ts      = Stream.emits(pandas).covary[IO]
      val action = hdp.delete(pathStr) >>
        ts.through(hdp.avroSink(pathStr, pandaSchema, CodecFactory.nullCodec)).compile.drain >>
        hdp.avroSource(pathStr, pandaSchema).compile.toList
      action.asserting(_ shouldBe pandas)
    }
  }
}
