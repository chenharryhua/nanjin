package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.{AvroTopic, KafkaContext, KafkaSettings}
import eu.timepit.refined.auto.*
import org.apache.kafka.clients.consumer.ConsumerConfig

package object kafka {

  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "nj-kafka-unit-test-group")
        .withStreamingProperty("state.dir", "./data/kafka_states"))

  val taxi: AvroTopic[Int, trip_record] = AvroTopic[Int, trip_record](TopicName("nyc_yellow_taxi_trip_data"))
}
