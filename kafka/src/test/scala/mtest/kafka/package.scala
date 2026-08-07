package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import com.github.chenharryhua.nanjin.kafka.serdes.{Primitive, Structured}
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, TopicDef, TopicName}
import org.apache.avro.generic.GenericRecord

package object kafka {

  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(_.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(_.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
        .withStreamingProperty(_.STATE_DIR_CONFIG, "./data/kafka_states")
        .withSerdeProperty(_.AUTO_REGISTER_SCHEMAS, "true")
    )

  val taxi: TopicDef[Integer, trip_record] =
    TopicDef("nyc_yellow_taxi_trip_data", Primitive[Integer], Structured[GenericRecord].become[trip_record])
}
