package mtest

import cats.derived.auto.show._
import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{
  GenericRecordDecoder,
  GenericRecordEncoder,
  JsonAvroDeserialization,
  JsonAvroSerialization
}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import cats.implicits._

class JsonAvroPipeTest extends AnyFunSuite {
  import TestData._
  val gser  = new GenericRecordEncoder[IO, Tigger]
  val gdser = new GenericRecordDecoder[IO, Tigger]
  val ser   = new JsonAvroSerialization[IO](AvroSchema[Tigger])
  val dser  = new JsonAvroDeserialization[IO](AvroSchema[Tigger])

  test("json-avro identity") {
    val data: Stream[IO, Tigger] = Stream.emits(list)

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
