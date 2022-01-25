package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.{BinaryAvroSerialization, GenericRecordCodec}
import com.github.chenharryhua.nanjin.terminals.{NJHadoop, NJPath}
import com.sksamuel.avro4s.AvroSchema
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.*

class BinaryAvroPipeTest extends AnyFunSuite {
  import TestData.*
  val gr                       = new GenericRecordCodec[IO, Tigger]
  val ba                       = new BinaryAvroSerialization[IO](AvroSchema[Tigger])
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("binary-json identity") {

    assert(
      data
        .through(gr.encode(Tigger.avroEncoder))
        .through(ba.serialize)
        .through(ba.deserialize)
        .through(gr.decode(Tigger.avroDecoder))
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }

  test("write/read identity") {
    val hd    = NJHadoop[IO](new Configuration())
    val path  = NJPath("data/pipe/bin-avro.avro")
    val write = data.through(gr.encode(Tigger.avroEncoder)).through(ba.serialize).through(hd.byteSink(path))
    val read  = hd.byteSource(path, 100.kb).through(ba.deserialize).through(gr.decode(Tigger.avroDecoder))
    val run   = write.compile.drain >> read.compile.toList
    assert(run.unsafeRunSync() === tiggers)
  }
}
