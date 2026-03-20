package com.github.chenharryhua.nanjin.kafka.serdes

import org.apache.kafka.common.serialization.{Serde, Serdes}

import java.nio.ByteBuffer
import java.util.UUID
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient

sealed trait Primitive[A] extends Unregistered[A]

object Primitive {
  inline def apply[A](using ev: Primitive[A]): Primitive[A] = ev

  given Primitive[String] = new Primitive[String] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[String] = Serdes.String()
  }

  given Primitive[Array[Byte]] = new Primitive[Array[Byte]] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[Array[Byte]] = Serdes.ByteArray()
  }

  given Primitive[java.lang.Long] = new Primitive[java.lang.Long] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Long] = Serdes.Long()
  }

  given Primitive[java.lang.Integer] = new Primitive[java.lang.Integer] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Integer] = Serdes.Integer()
  }

  given Primitive[java.lang.Float] = new Primitive[java.lang.Float] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Float] = Serdes.Float()
  }

  given Primitive[java.lang.Double] = new Primitive[java.lang.Double] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Double] = Serdes.Double()
  }

  given Primitive[java.lang.Short] = new Primitive[java.lang.Short] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Short] = Serdes.Short()
  }

  given Primitive[java.lang.Boolean] = new Primitive[java.lang.Boolean] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[java.lang.Boolean] = Serdes.Boolean()
  }

  given Primitive[ByteBuffer] = new Primitive[ByteBuffer] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[ByteBuffer] = Serdes.ByteBuffer()
  }

  given Primitive[UUID] = new Primitive[UUID] {
    override def registerWith(srClient: SchemaRegistryClient): Serde[UUID] = Serdes.UUID()
  }
}
