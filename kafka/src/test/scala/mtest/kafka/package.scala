package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.serdes.{AvroBase, Primitive}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings, TopicDef, TopicName}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.avro.generic.GenericRecord
import com.github.chenharryhua.nanjin.kafka.serdes.avro4s

package object kafka {

  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
        .withStreamingProperty("state.dir", "./data/kafka_states"))

  val taxi: TopicDef[Integer, trip_record] =
    TopicDef("nyc_yellow_taxi_trip_data", Primitive[Integer], AvroBase[GenericRecord].iso(avro4s[trip_record]))
}
