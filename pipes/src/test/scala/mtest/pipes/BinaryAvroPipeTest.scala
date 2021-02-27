package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerialization, GenericRecordCodec}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class BinaryAvroPipeTest extends AnyFunSuite {
  import TestData._
  val gr                       = new GenericRecordCodec[IO, Tigger]
  val ba                       = new BinaryAvroSerialization[IO](AvroSchema[Tigger])
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("identity") {
    val run = data
      .through(gr.encode(Tigger.avroEncoder))
      .through(ba.serialize)
      .through(ba.deserialize)
      .through(gr.decode(Tigger.avroDecoder))
      .compile
      .toList
    assert(run.unsafeRunSync() == tiggers)
  }
}
