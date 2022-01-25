package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CsvSerialization
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.sksamuel.avro4s.AvroSchema
import eu.timepit.refined.auto.*
import fs2.Stream
import kantan.csv.CsvConfiguration
import org.apache.avro.file.CodecFactory
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class CsvPipeTest extends AnyFunSuite {
  import TestData.*
  val ser                      = new CsvSerialization[IO, Tigger](CsvConfiguration.rfc)
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)
  test("csv identity") {

    assert(data.through(ser.serialize(300.kb)).through(ser.deserialize(5)).compile.toList.unsafeRunSync() === tiggers)
  }

  test("write/read identity csv") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/csv.csv")
    val write = data.through(ser.serialize(2.kb)).through(hd.byteSink(path))
    val read  = hd.byteSource(path, 1.kb).through(ser.deserialize(1))
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tiggers)
  }
}
