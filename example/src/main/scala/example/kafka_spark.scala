package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.spark.{SparKafkaContext, SparkSessionExt, SparkSettings}
import eu.timepit.refined.auto.*
import fs2.kafka.Acks
import io.lemonlabs.uri.Url
import org.apache.spark.sql.SparkSession

object kafka_spark {
  val spark: SparkSession = SparkSettings(sydneyTime).sparkSession
  val sparKafka: SparKafkaContext[IO] = spark.alongWith[IO](kafka_connector_s3.ctx)

  val path: Url = Url.parse("s3a://bucket_name/folder_name")
  val topic: TopicName = TopicName("any.kafka.topic")

  // batch dump a kafka topic
  sparKafka.dump(topic, path)

  // batch dump a kafka topic with date range
  private val dateRange = DateTimeRange(sydneyTime).withYesterday
  sparKafka.dump(topic, path, dateRange)

  // load saved data into kafka
  sparKafka.upload(topic, path, 1000, _.withAcks(Acks.One))

  // dataset statistics
  sparKafka.stats.jackson(path)
}
