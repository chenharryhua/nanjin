package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.BinaryAvroSerde
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class BinaryAvroPipeTest extends AnyFunSuite {
  import TestData.*
  val encoder: ToRecord[Tigger] = ToRecord[Tigger](Tigger.avroEncoder)
  val data: Stream[IO, Tigger]  = Stream.emits(tiggers)

  test("binary-json identity") {

    assert(
      data
        .map(encoder.to)
        .through(BinaryAvroSerde.serialize[IO](AvroSchema[Tigger]))
        .through(BinaryAvroSerde.deserialize[IO](AvroSchema[Tigger]))
        .map(Tigger.avroDecoder.decode)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }

  test("write/read identity") {
    val hd   = NJHadoop[IO](new Configuration())
    val path = NJPath("data/pipe/bin-avro.avro")
    val write =
      data.map(encoder.to).through(BinaryAvroSerde.serialize[IO](AvroSchema[Tigger])).through(hd.byteSink(path))
    val read = hd
      .byteSource(path, 100.kb)
      .through(BinaryAvroSerde.deserialize[IO](AvroSchema[Tigger]))
      .map(Tigger.avroDecoder.decode)
    val run = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tiggers)
  }
}
