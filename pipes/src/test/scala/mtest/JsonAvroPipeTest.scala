package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{GenericRecordCodec, JacksonSerialization}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class JsonAvroPipeTest extends AnyFunSuite {
  import TestData._
  val gser = new GenericRecordCodec[IO, Tigger]
  val ser  = new JacksonSerialization[IO](AvroSchema[Tigger])

  test("json-avro identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(
      data
        .through(gser.encode)
        .through(ser.serialize)
        .through(ser.deserialize)
        .through(gser.decode)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
  test("jackson-compact-string size") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(
      data.through(gser.encode).through(ser.compactJson).compile.toList.unsafeRunSync().size == 10)
  }
  test("jackson-pretty-string size") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(
      data.through(gser.encode).through(ser.prettyJson).compile.toList.unsafeRunSync().size == 10)
  }

}
