package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.serdes.{isoGenericRecord, Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings, TopicDef, TopicName}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.avro.generic.GenericRecord

package object kafka {

  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
        .withStreamingProperty("state.dir", "./data/kafka_states")
        .withSchemaRegistryProperty("auto.register.schemas", "true")
    )

  val taxi: TopicDef[Integer, trip_record] =
    TopicDef(
      "nyc_yellow_taxi_trip_data",
      Primitive[Integer],
      Structured[GenericRecord].iso(isoGenericRecord[trip_record]))
}
