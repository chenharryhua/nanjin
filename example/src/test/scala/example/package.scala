import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings

package object example {

  val ctx: KafkaContext[IO] =
    KafkaContext[IO](
      KafkaSettings.local
        .withConsumerProperty(_.AUTO_OFFSET_RESET_CONFIG, "earliest")
        .withConsumerProperty(_.GROUP_ID_CONFIG, "example"))

}
