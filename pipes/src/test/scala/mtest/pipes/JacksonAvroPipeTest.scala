package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.jackson
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import fs2.Stream
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
class JacksonAvroPipeTest extends AnyFunSuite {
  import mtest.terminals.TestData.*
  val encoder: ToRecord[Tiger] = ToRecord[Tiger](Tiger.avroEncoder)
  val schema: Schema = AvroSchema[Tiger]
  val data: Stream[IO, Tiger] = Stream.emits(tigers)

  test("json-avro identity") {
    assert(
      data
        .map(encoder.to)
        .through(jackson.toBytes(schema))
        .through(jackson.fromBytes(schema))
        .map(Tiger.avroDecoder.decode)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }

}
