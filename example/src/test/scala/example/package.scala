import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import org.apache.kafka.clients.consumer.ConsumerConfig
 
package object example {

  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "example"))

}
