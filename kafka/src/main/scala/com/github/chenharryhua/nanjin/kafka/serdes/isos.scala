package com.github.chenharryhua.nanjin.kafka.serdes

import com.fasterxml.jackson.databind.JsonNode
import com.google.protobuf.DynamicMessage
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.circe.{Json, Printer}
import io.scalaland.chimney.{Iso, Transformer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import java.nio.ByteBuffer
import scala.reflect.ClassTag

/*
 * This Iso is lawful assuming Avro4s Encoder/Decoder form a true isomorphism
 * between A and GenericRecord under the same schema.
 *
 * In practice, this holds when records are produced and consumed using the
 * same schema and codec, without schema evolution.
 */
def avro4s[A: {SchemaFor, Decoder, Encoder}]: Iso[GenericRecord, A] = {
  val schema: Schema = SchemaFor[A].schema
  val dec: FromRecord[A] = FromRecord[A](schema)
  val enc: ToRecord[A] = ToRecord[A](schema)
  val first: Transformer[GenericRecord, A] = new Transformer[GenericRecord, A]:
    override def transform(src: GenericRecord): A = dec.from(src)

  val second: Transformer[A, GenericRecord] = new Transformer[A, GenericRecord]:
    override def transform(src: A): GenericRecord = enc.to(src)

  Iso[GenericRecord, A](first, second)
}

def protobuf[A <: GeneratedMessage](using gmc: GeneratedMessageCompanion[A]): Iso[DynamicMessage, A] = {
  val first = new Transformer[DynamicMessage, A]:
    override def transform(src: DynamicMessage): A =
      gmc.parseFrom(src.toByteArray)

  val second = new Transformer[A, DynamicMessage]:
    override def transform(src: A): DynamicMessage =
      DynamicMessage.parseFrom(src.companion.javaDescriptor, src.toByteArray)

  Iso(first, second)
}

def jackson[A: ClassTag]: Iso[JsonNode, A] = {
  val first = new Transformer[JsonNode, A]:
    override def transform(src: JsonNode): A =
      globalObjectMapper.convertValue[A](src)

  val second = new Transformer[A, JsonNode]:
    override def transform(src: A): JsonNode =
      globalObjectMapper.valueToTree[JsonNode](src)

  Iso(first, second)
}

def jsonByteBuffer: Iso[ByteBuffer, Json] = {
  val first = new Transformer[ByteBuffer, Json]:
    override def transform(src: ByteBuffer): Json =
      io.circe.jawn.parseByteBuffer(src) match {
        case Left(ex)     => throw ex // scalafix:ok
        case Right(value) => value
      }

  val second = new Transformer[Json, ByteBuffer]:
    override def transform(src: Json): ByteBuffer =
      Printer.noSpaces.printToByteBuffer(src)

  Iso(first, second)
}
