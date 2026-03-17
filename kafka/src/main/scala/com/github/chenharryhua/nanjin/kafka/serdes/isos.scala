package com.github.chenharryhua.nanjin.kafka.serdes

import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.scalaland.chimney.{Iso, Transformer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

def avro4s[A: {SchemaFor, Decoder, Encoder}]: Iso[GenericRecord, A] = {
  val schema: Schema = SchemaFor[A].schema
  val dec: FromRecord[A] = FromRecord[A](schema)
  val enc: ToRecord[A] = ToRecord[A](schema)
  val first: Transformer[GenericRecord, A] = new Transformer[GenericRecord, A] {
    override def transform(src: GenericRecord): A = dec.from(src)
  }
  val second: Transformer[A, GenericRecord] = new Transformer[A, GenericRecord] {
    override def transform(src: A): GenericRecord = enc.to(src)
  }

  Iso[GenericRecord, A](first, second)
}
