package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.TopicName
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.IO
import com.fasterxml.jackson.databind.JsonNode
import com.github.chenharryhua.nanjin.kafka.record.NJConsumerRecord
import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Structured}
import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{Decoder, Encoder, SchemaFor}
import org.apache.avro.generic.GenericRecord

class TopicSyntaxTest extends AnyFunSuite {
  test("topic name") {
    val tn: TopicName = "abc.unsafe"
    val tn2 = TopicName("abc.checked")
    println((tn, tn2))
  }

  test("consume") {
    val k = ctx.asKey(Primitive[Integer]).deserializer[IO].attempt
    val v = ctx.asValue(Primitive[Integer]).deserializer[IO].option

    val res = ctx.consume("topic", k, v)
    println(res)
  }

  test("producer") {
    val k = ctx.asKey(Primitive[Integer].imap(identity)(identity)).serializer[IO]
    val v = ctx.asValue(Primitive[Integer]).serializer[IO].option

    val res = ctx.produce("topic", k, v)
    println(res)
  }

  test("schema-based") {
    Structured[JsonNode]
    Structured[GenericRecord]
    Structured[DynamicMessage]
  }

  test("nj consumer record basic") {
    summon[SchemaFor[NJConsumerRecord[Int, Int]]]
    summon[Decoder[NJConsumerRecord[Int, Int]]]
    summon[Encoder[NJConsumerRecord[Int, Int]]]
  }

  test("nj consumer record foo") {
    val s = summon[SchemaFor[NJConsumerRecord[Int, Foo]]]
    summon[Decoder[NJConsumerRecord[Int, Foo]]]
    summon[Encoder[NJConsumerRecord[Int, Foo]]]
    println(s.schema)
  }
}
