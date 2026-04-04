package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Registered, Structured}
import fs2.kafka.{GenericDeserializer, Headers, Key, Value}
import io.circe.Json
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

import java.nio.ByteBuffer

final case class Foo(a: Int, b: String)

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

  test("null - deserialize") {
    val s1 = Structured[GenericRecord].become[Foo]
    assertThrows[NullPointerException](ctx.asValue(s1).serde.deserializer().deserialize("t", null))
    val s2 = Structured[GenericRecord].option.become[Option[Foo]].orNull
    assert(ctx.asValue(s2).serde.deserializer().deserialize("t", null) == null)
  }

  test("null - serialize") {
    val s1 = Structured[GenericRecord].become[Foo]
    assertThrows[NullPointerException](ctx.asValue(s1).serde.serializer().serialize("t", null))
    val s2 = Structured[GenericRecord].option.become[Option[Foo]].orNull
    assert(ctx.asValue(s2).serde.serializer().serialize("t", null) == null)
  }

  test("attempt - should not throw exception") {
    val s1 = Primitive[ByteBuffer].become[Json]
    val deSer = ctx.asValue(s1).deserializer[IO].map(_.attempt).use(
      _.deserialize("a", Headers.empty, Array(1, 2, 3))).unsafeRunSync()
    assert(deSer.isLeft)
  }

  test("primitive") {
    val s1 = Primitive[Integer].become[Option[Int]]
    val List(_, _, _, x) =
      ctx.asValue(s1).serializer[IO].use(_.serialize("a", Headers.empty, Some(10))).unsafeRunSync().toList
    assert(x === 10)
    val y = ctx.asValue(s1).serializer[IO].use(_.serialize("a", Headers.empty, None)).unsafeRunSync()
    println(y === null)
  }
}
