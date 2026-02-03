package example

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.datetime.DateTimeRange
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.messages.kafka.codec.AvroFor
import com.github.chenharryhua.nanjin.spark.{SparKafkaContext, SparkSessionExt, SparkSettings}
import com.github.chenharryhua.nanjin.spark.kafka.TopicSummary
import eu.timepit.refined.auto.*
import fs2.kafka.Acks
import io.lemonlabs.uri.Url
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.DurationInt

object kafka_spark {
  val spark: SparkSession = SparkSettings(sydneyTime).sparkSession
  val sparKafka: SparKafkaContext[IO] = spark.alongWith[IO](kafka_connector_s3.ctx)

  val path: Url = Url.parse("s3a://bucket_name/folder_name")
  val topic: AvroTopic[Int,AvroFor.FromBroker] = AvroTopic[Int, AvroFor.FromBroker](TopicName("any.kafka.topic"))

  val dr: DateTimeRange = DateTimeRange(sydneyTime)

  // batch dump a kafka topic
  val dump: IO[Long] = sparKafka.dump(topic, path, _.withConsumer(_.withGroupId("gid")))

  val dumpCirce: IO[Long] = sparKafka.dumpCirce(topic, path, _.isIgnoreError(true).withDateTimeRange(dr))

  // load saved data into kafka
  val load: IO[Long] = sparKafka.upload(topic, path, _.withProducer(_.withAcks(Acks.One)).withTimeout(3.seconds))

  // dataset statistics summary
  val stats: IO[Option[TopicSummary]] = sparKafka.stats.jackson(path).flatMap(_.summary("test"))
}
