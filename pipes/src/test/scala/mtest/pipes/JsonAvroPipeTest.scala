package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.{GenericRecordCodec, JacksonSerialization}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class JsonAvroPipeTest extends AnyFunSuite {
  import TestData._
  val gser                     = new GenericRecordCodec[IO, Tigger]
  val ser                      = new JacksonSerialization[IO](AvroSchema[Tigger])
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)
  test("json-avro identity") {
    val run = data
      .through(gser.encode(Tigger.avroEncoder))
      .through(ser.serialize)
      .through(ser.deserialize)
      .through(gser.decode(Tigger.avroDecoder))
      .compile
      .toList
    assert(run.unsafeRunSync() == tiggers)
  }

  test("jackson-compact-string size") {
    val run = data.through(gser.encode(Tigger.avroEncoder)).through(ser.compactJson).compile.toList

    assert(run.unsafeRunSync().size == 10)
  }

  test("jackson-pretty-string size") {
    val run = data.through(gser.encode(Tigger.avroEncoder)).through(ser.prettyJson).compile.toList
    assert(run.unsafeRunSync().size == 10)
  }
}
