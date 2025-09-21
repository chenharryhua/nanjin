package com.github.chenharryhua.nanjin.messages.kafka.codec

import com.sksamuel.avro4s.{Decoder as AvroDecoder, Encoder as AvroEncoder, SchemaFor}
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

import java.nio.ByteBuffer
import java.util
import java.util.UUID

/*
 * spark friendly
 */
trait AvroFor[A] extends RegisterSerde[A] {
  def avroCodec: AvroCodec[A]
  protected def unregisteredSerde: Serde[A]
}

private[codec] trait LowerPriority {
  implicit def avro4sCodec[A: SchemaFor: AvroEncoder: AvroDecoder]: AvroFor[A] =
    AvroFor(AvroCodec[A])
}

object AvroFor extends LowerPriority {
  def apply[A](implicit ev: AvroFor[A]): AvroFor[A] = ev

  def apply[A](codec: AvroCodec[A]): AvroFor[A] =
    new AvroFor[A] {
      override val avroCodec: AvroCodec[A] = codec

      override protected val unregisteredSerde: Serde[A] = new Serde[A] with Serializable {
        override val serializer: Serializer[A] =
          new Serializer[A] with Serializable {
            @transient private[this] lazy val ser: GenericAvroSerializer = new GenericAvroSerializer
            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            override def serialize(topic: String, data: A): Array[Byte] =
              if (data == null) null
              else
                avroCodec.encode(data) match {
                  case gr: GenericRecord => ser.serialize(topic, gr)

                  case ex => sys.error(s"${ex.getClass.toString} is not Generic Record")
                }
          }

        override val deserializer: Deserializer[A] =
          new Deserializer[A] with Serializable {

            @transient private[this] lazy val deSer: GenericAvroDeserializer =
              new GenericAvroDeserializer

            override def close(): Unit =
              deSer.close()

            override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            @SuppressWarnings(Array("AsInstanceOf"))
            override def deserialize(topic: String, data: Array[Byte]): A =
              if (data == null) null.asInstanceOf[A]
              else avroCodec.decode(deSer.deserialize(topic, data))
          }
      }
    }

// 1: String
  implicit object avroForString extends AvroFor[String] {
    override val avroCodec: AvroCodec[String] = AvroCodec[String]
    override protected val unregisteredSerde: Serde[String] = serializable.stringSerde
  }

  // 2: Long
  implicit object avroForLong extends AvroFor[Long] {
    override val avroCodec: AvroCodec[Long] = AvroCodec[Long]
    override protected val unregisteredSerde: Serde[Long] = serializable.longSerde
  }

// 3: array byte
  implicit object avroForArrayByte extends AvroFor[Array[Byte]] {
    override val avroCodec: AvroCodec[Array[Byte]] = AvroCodec[Array[Byte]]
    override protected val unregisteredSerde: Serde[Array[Byte]] = serializable.byteArraySerde
  }

  // 4: byte buffer
  implicit object avroForByteBuffer extends AvroFor[ByteBuffer] {
    override val avroCodec: AvroCodec[ByteBuffer] = AvroCodec[ByteBuffer]
    override protected val unregisteredSerde: Serde[ByteBuffer] = serializable.byteBufferSerde
  }

  // 5: short
  implicit object avroForShort extends AvroFor[Short] {
    override val avroCodec: AvroCodec[Short] = AvroCodec[Short]
    override protected val unregisteredSerde: Serde[Short] = serializable.shortSerde
  }

  // 6: float
  implicit object avroForFloat extends AvroFor[Float] {
    override val avroCodec: AvroCodec[Float] = AvroCodec[Float]
    override protected val unregisteredSerde: Serde[Float] = serializable.floatSerde
  }

  // 7: double
  implicit object avroForDouble extends AvroFor[Double] {
    override val avroCodec: AvroCodec[Double] = AvroCodec[Double]
    override protected val unregisteredSerde: Serde[Double] = serializable.doubleSerde
  }

  // 8: int
  implicit object avroForInt extends AvroFor[Int] {
    override val avroCodec: AvroCodec[Int] = AvroCodec[Int]
    override protected val unregisteredSerde: Serde[Int] = serializable.intSerde
  }

  // 9: uuid
  implicit object avroForUUID extends AvroFor[UUID] {
    override val avroCodec: AvroCodec[UUID] = AvroCodec[UUID]
    override protected val unregisteredSerde: Serde[UUID] = serializable.uuidSerde
  }

  // 10. generic record
  implicit object avroForGenericRecord extends AvroFor[GenericRecord] {

    override val avroCodec: AvroCodec[GenericRecord] = {
      val sf: SchemaFor[GenericRecord] = SchemaFor[GenericRecord](Schema.create(Schema.Type.NULL))
      val dc: AvroDecoder[GenericRecord] = new AvroDecoder[GenericRecord] {
        override def decode(value: Any): GenericRecord = value match {
          case gr: GenericRecord => gr
          case ex                => sys.error(s"${ex.getClass.toString} is not Generic Record")
        }
        override def schemaFor: SchemaFor[GenericRecord] = sf
      }
      val ec: AvroEncoder[GenericRecord] = new AvroEncoder[GenericRecord] {
        override def encode(value: GenericRecord): AnyRef = value
        override def schemaFor: SchemaFor[GenericRecord] = sf
      }
      AvroCodec(sf, dc, ec)
    }

    override protected val unregisteredSerde: Serde[GenericRecord] = new Serde[GenericRecord]
      with Serializable {
      override val serializer: Serializer[GenericRecord] =
        new Serializer[GenericRecord] with Serializable {
          @transient private[this] lazy val ser: GenericAvroSerializer = new GenericAvroSerializer
          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            ser.configure(configs, isKey)

          override def close(): Unit = ser.close()

          override def serialize(topic: String, data: GenericRecord): Array[Byte] = ser.serialize(topic, data)
        }

      override val deserializer: Deserializer[GenericRecord] =
        new Deserializer[GenericRecord] with Serializable {

          @transient private[this] lazy val deSer: GenericAvroDeserializer =
            new GenericAvroDeserializer

          override def close(): Unit = deSer.close()

          override def configure(configs: util.Map[String, ?], isKey: Boolean): Unit =
            deSer.configure(configs, isKey)

          override def deserialize(topic: String, data: Array[Byte]): GenericRecord =
            deSer.deserialize(topic, data)
        }
    }
  }
}
