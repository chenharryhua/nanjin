package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{
  BinaryAvroDeserialization,
  BinaryAvroSerialization,
  GenericRecordDecoder,
  GenericRecordEncoder
}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class BinaryAvroPipeTest extends AnyFunSuite {
  import TestData._
  val gser  = new GenericRecordEncoder[IO, Tigger]
  val gdser = new GenericRecordDecoder[IO, Tigger]
  val ser   = new BinaryAvroSerialization[IO](AvroSchema[Tigger])
  val dser  = new BinaryAvroDeserialization[IO](AvroSchema[Tigger])

  test("binary-json identity") {
    val data: Stream[IO, Tigger] = Stream.fromIterator[IO](list.iterator)

    assert(
      data
        .through(gser.encode)
        .through(ser.serialize)
        .through(dser.deserialize)
        .through(gdser.decode)
        .compile
        .toList
        .unsafeRunSync() === list)
  }
}
