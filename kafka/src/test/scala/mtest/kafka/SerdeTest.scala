package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.serdes.{ SchemaBased, Primitive, Registered}
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import fs2.kafka.{GenericDeserializer, Key, KeyDeserializer, Value}
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

final case class Abc(a: Int, b: String, c: Long)

class SerdeTest extends AnyFunSuite {
  test("avro primitive") {
    val avro: Registered[Key, GenericRecord] =
      summon[SchemaBased[GenericRecord]].imap(identity)(identity).asKey(Map.empty)
    println(avro)
  }

  test("poly") {
    val k = summon[Primitive[java.lang.Long]].asKey(Map.empty)
    val v = summon[Primitive[UUID]].asValue(Map.empty)
    println((k, v))
  }

  test("avro4s") {
    val s = SchemaFor[Abc]
    val d = ToRecord[Abc](s.schema)
    val e = FromRecord[Abc](s.schema)
    val serde =
      summon[SchemaBased[GenericRecord]].imap(e.from)(d.to).asKey(Map.empty)

    val deSerde: KeyDeserializer[IO, Abc] = serde.deserializer[IO]

    println(deSerde)

  }

  test("GenericDeserializer") {
    val k: GenericDeserializer[Key, IO, Array[Byte]] = GenericDeserializer.identity[IO]
    val v: GenericDeserializer[Value, IO, Array[Byte]] = GenericDeserializer.identity[IO]

    println((k, v))
  }

}
