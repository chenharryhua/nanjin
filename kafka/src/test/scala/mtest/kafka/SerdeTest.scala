package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Registered, Structured}
import fs2.kafka.{GenericDeserializer, Headers, Key, Value}
import io.circe.{Codec, Json}
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

final case class Foo(a: Int, b: String) derives Codec.AsObject

class SerdeTest extends AnyFunSuite {
  test("1.avro primitive") {
    val avro: Registered[Key, GenericRecord] = ctx.asKey(Structured[GenericRecord])
    println(avro)
  }

  test("2.GenericDeserializer") {
    val k: GenericDeserializer[Key, IO, Array[Byte]] = GenericDeserializer.identity[IO]
    val v: GenericDeserializer[Value, IO, Array[Byte]] = GenericDeserializer.identity[IO]

    println((k, v))
  }

  test("3.null - deserialize") {
    val s1 = Structured[GenericRecord].become[Foo]
    assertThrows[NullPointerException](ctx.asValue(s1).serde.deserializer().deserialize("t", null))
    val s2 = Structured[GenericRecord].option.become[Option[Foo]].orNull
    assert(ctx.asValue(s2).serde.deserializer().deserialize("t", null) == null)
  }

  test("4.null - serialize") {
    val s1 = Structured[GenericRecord].become[Foo]
    assertThrows[NullPointerException](ctx.asValue(s1).serde.serializer().serialize("t", null))
    val s2 = Structured[GenericRecord].option.become[Option[Foo]].orNull
    assert(ctx.asValue(s2).serde.serializer().serialize("t", null) == null)
  }

  test("5.attempt - should not throw exception") {
    val s1 = Structured[Json]
    val deSer = ctx.asValue(s1).deserializer[IO].map(_.attempt).use(
      _.deserialize("a", Headers.empty, Array(1, 2, 3))).unsafeRunSync()
    assert(deSer.isLeft)
  }

  test("6.primitive") {
    val s1 = Primitive[Integer].become[Option[Int]]
    val List(_, _, _, x) =
      ctx.asValue(s1).serializer[IO].use(_.serialize("a", Headers.empty, Some(10))).unsafeRunSync().toList
    assert(x === 10)
    val y = ctx.asValue(s1).serializer[IO].use(_.serialize("a", Headers.empty, None)).unsafeRunSync()
    println(y === null)
  }
}
