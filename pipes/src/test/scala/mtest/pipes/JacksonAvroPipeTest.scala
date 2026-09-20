package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.jackson
import com.sksamuel.avro4s.{AvroSchema, ToRecord}
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.avro.Schema
class JacksonAvroPipeTest extends CatsEffectSuite {
  import mtest.terminals.TestData.*
  val encoder: ToRecord[Tiger] = Tiger.to
  val schema: Schema = AvroSchema[Tiger]
  val data: Stream[IO, Tiger] = Stream.emits(tigers)

  test("1.json-avro identity") {
    data
      .map(encoder.to)
      .through(jackson.toBytes(schema))
      .through(jackson.fromBytes(schema))
      .map(Tiger.from.from)
      .compile
      .toList
      .map(res => assert(res == tigers))
  }

}
