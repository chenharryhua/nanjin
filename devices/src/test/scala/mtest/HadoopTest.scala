package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.devices.NJHadoop
import fs2.Stream
import fs2.io.readInputStream
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

object HadoopTestData {
  final case class Panda(name: String, age: Int)
  val pandas: List[Panda] = List(Panda("aa", 0), Panda("bb", 1), Panda("cc", 2))
}

class HadoopTest extends AnyFunSuite {
  val hdp = new NJHadoop[IO](new Configuration(), blocker)

  test("hadoop text write/read identity") {
    val pathStr    = "./data/devices/hadoop/test.txt"
    val testString = "save string to hadoop"
    val ts: Stream[IO, Byte] =
      Stream(testString).through(fs2.text.utf8Encode)

    val action = hdp.delete(pathStr) >>
      ts.through(hdp.sink(pathStr)).compile.drain >>
      hdp
        .inputStream(pathStr)
        .flatMap(is => readInputStream(IO(is), 64, blocker).through(fs2.text.utf8Decode))
        .compile
        .toList
    assert(action.unsafeRunSync.head == testString)
  }

  test("parquet write/read indentity") {
    import HadoopTestData._
    val pathStr = "./data/devices/parquet/test.parquet"
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.parquetSink[Panda](pathStr)).compile.drain >>
      hdp.parquetSource[Panda](pathStr).compile.toList
    assert(action.unsafeRunSync == pandas)
  }

  test("avro write/read indentity") {
    import HadoopTestData._
    val pathStr = "./data/devices/avro/test.avro"
    val ts      = Stream.emits(pandas).covary[IO]
    val action = hdp.delete(pathStr) >>
      ts.through(hdp.avroSink[Panda](pathStr)).compile.drain >>
      hdp.avroSource[Panda](pathStr).compile.toList
    assert(action.unsafeRunSync == pandas)
  }
}
