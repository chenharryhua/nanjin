package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{GenericRecordDecoder, GenericRecordEncoder}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import com.sksamuel.avro4s.Encoder
import com.sksamuel.avro4s.Decoder

class GenericRecordPipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new GenericRecordEncoder[IO, Tigger](Encoder[Tigger])
  val dser = new GenericRecordDecoder[IO, Tigger](Decoder[Tigger])

  test("generic-record identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(data.through(ser.encode).through(dser.decode).compile.toList.unsafeRunSync() === tiggers)
  }
}
