import cats.effect.IO
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.kafka.{KafkaContext, KafkaSettings}
import com.github.chenharryhua.nanjin.spark.*
import com.github.chenharryhua.nanjin.terminals.NJHadoop
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession

package object example {

  lazy val sparkSession: SparkSession = SparkSettings(sydneyTime).sparkSession

  val ctx: KafkaContext[IO] =
    KafkaSettings.local
      .withApplicationId("nj-example-app")
      .withGroupId("nj-example-group")
      .withConsumerProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withClientID("nj-example")
      .ioContext

  val sparKafka: SparKafkaContext[IO] = sparkSession.alongWith(ctx)
  val hadoop: NJHadoop[IO] = sparkSession.hadoop[IO]
}
