package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CsvSerde
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
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)
  test("csv identity") {

    assert(
      data
        .through(CsvSerde.serialize[IO, Tigger](CsvConfiguration.rfc, 300.kb))
        .through(CsvSerde.deserialize[IO, Tigger](CsvConfiguration.rfc, 5))
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }

  test("write/read identity csv") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/csv.csv")
    val write = data.through(CsvSerde.serialize[IO, Tigger](CsvConfiguration.rfc, 2.kb)).through(hd.byteSink(path))
    val read  = hd.byteSource(path, 1.kb).through(CsvSerde.deserialize[IO, Tigger](CsvConfiguration.rfc, 1))
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tiggers)
  }
}
