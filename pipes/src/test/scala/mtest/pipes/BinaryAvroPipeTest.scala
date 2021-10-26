package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.{BinaryAvroSerialization, GenericRecordCodec}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class BinaryAvroPipeTest extends AnyFunSuite {
  import TestData.*
  val gr = new GenericRecordCodec[IO, Tigger]
  val ba = new BinaryAvroSerialization[IO](AvroSchema[Tigger])

  test("binary-json identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(
      data
        .through(gr.encode(Tigger.avroEncoder))
        .through(ba.serialize(100))
        .through(ba.deserialize)
        .through(gr.decode(Tigger.avroDecoder))
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
