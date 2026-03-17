package com.github.chenharryhua.nanjin.kafka.serdes

import org.apache.kafka.common.serialization.{Serde, Serdes}

import java.nio.ByteBuffer
import java.util.UUID

sealed trait Primitive[A] extends Unregistered[A]

object Primitive {

  given Primitive[String] = new Primitive[String] {
    override def unregistered: Serde[String] = Serdes.String()
  }

  given Primitive[Array[Byte]] = new Primitive[Array[Byte]] {
    override def unregistered: Serde[Array[Byte]] = Serdes.ByteArray()
  }

  given Primitive[java.lang.Long] = new Primitive[java.lang.Long] {
    override def unregistered: Serde[java.lang.Long] = Serdes.Long()
  }

  given Primitive[java.lang.Integer] = new Primitive[java.lang.Integer] {
    override def unregistered: Serde[java.lang.Integer] = Serdes.Integer()
  }

  given Primitive[java.lang.Float] = new Primitive[java.lang.Float] {
    override def unregistered: Serde[java.lang.Float] = Serdes.Float()
  }

  given Primitive[java.lang.Double] = new Primitive[java.lang.Double] {
    override def unregistered: Serde[java.lang.Double] = Serdes.Double()
  }

  given Primitive[java.lang.Short] = new Primitive[java.lang.Short] {
    override def unregistered: Serde[java.lang.Short] = Serdes.Short()
  }

  given Primitive[java.lang.Boolean] = new Primitive[java.lang.Boolean] {
    override def unregistered: Serde[java.lang.Boolean] = Serdes.Boolean()
  }

  given Primitive[ByteBuffer] = new Primitive[ByteBuffer] {
    override def unregistered: Serde[ByteBuffer] = Serdes.ByteBuffer()
  }

  given Primitive[UUID] = new Primitive[UUID] {
    override def unregistered: Serde[UUID] = Serdes.UUID()
  }
}
