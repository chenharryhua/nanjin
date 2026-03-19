package mtest.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.serdes.{Registered, Structured}
import fs2.kafka.{GenericDeserializer, Key, Value}
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

final case class Foo(a: Int, b: String, c: Long)

class SerdeTest extends AnyFunSuite {
  test("avro primitive") {
    val avro: Registered[Key, GenericRecord] = ctx.asKey(Structured[GenericRecord])
    println(avro)
  }

  test("GenericDeserializer") {
    val k: GenericDeserializer[Key, IO, Array[Byte]] = GenericDeserializer.identity[IO]
    val v: GenericDeserializer[Value, IO, Array[Byte]] = GenericDeserializer.identity[IO]

    println((k, v))
  }

}
