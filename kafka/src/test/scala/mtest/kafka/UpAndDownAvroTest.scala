package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.kafka.schema.KafkaAvroSchema
import com.github.chenharryhua.nanjin.kafka.serdes.{isoGenericRecord, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{TopicDef, TopicName}
import com.sksamuel.avro4s.SchemaFor
import org.apache.avro.generic.GenericRecord
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.duration.DurationInt

final case class UpAndDown(a: Int, b: String)

class UpAndDownAvroTest extends AnyFunSuite {
  private val topic = TopicName("up.and.down.avro")
  private val avro: TopicDef[Integer, UpAndDown] =
    TopicDef(topic, Primitive[Integer], Structured[GenericRecord].iso(isoGenericRecord[UpAndDown]))

  test("avro - schema register") {
    val schema = summon[KafkaAvroSchema[UpAndDown]].schema
    println(schema)
    ctx.schemaRegistry
      .register(topic, value = Some(schema))
      .debug()
      .unsafeRunSync()
  }

  test("avro - produce") {
    ctx.produce(avro).produceOne(1, UpAndDown(1, "a")).flatMap(IO.println).unsafeRunSync()
  }

  test("avro - consume") {
    ctx.consume(avro).subscribe.take(1).debug().timeout(3.seconds).compile.drain.unsafeRunSync()
  }

}
