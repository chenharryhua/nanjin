package com.github.chenharryhua.nanjin.kafka.serdes

import com.github.chenharryhua.nanjin.kafka.serdes.Unregistered
import com.google.protobuf.DynamicMessage
import io.confluent.kafka.serializers.protobuf.{KafkaProtobufDeserializer, KafkaProtobufSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

trait ProtoBase[A] extends Unregistered[A]

object ProtoBase {
  given ProtoBase[DynamicMessage] = new ProtoBase[DynamicMessage] {
    override protected val unregistered: Serde[DynamicMessage] =
      new Serde[DynamicMessage] {
        /*
         * Serializer
         */
        override val serializer: Serializer[DynamicMessage] =
          new Serializer[DynamicMessage] {
            private val ser = new KafkaProtobufSerializer[DynamicMessage]

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              ser.configure(configs, isKey)

            override def close(): Unit = ser.close()

            export ser.serialize
          }

        /*
         * Deserializer
         */
        override val deserializer: Deserializer[DynamicMessage] =
          new Deserializer[DynamicMessage] {
            private val deSer = new KafkaProtobufDeserializer[DynamicMessage]

            override def configure(configs: java.util.Map[String, ?], isKey: Boolean): Unit =
              deSer.configure(configs, isKey)

            override def close(): Unit = deSer.close()

            export deSer.deserialize
          }
      }
  }
}
