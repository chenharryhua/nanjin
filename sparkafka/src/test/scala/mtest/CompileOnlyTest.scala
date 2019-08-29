package mtest

import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.sparkafka.SparkafkaDataset
import cats.implicits._

class CompileOnlyTest {
  val end   = LocalDateTime.now()
  val start = end.minusYears(1)

  spark.use { implicit s =>
    SparkafkaDataset.safeDataset(topics.ss, start, end) >>
      SparkafkaDataset.safeDataset(topics.si, start, end) >>
      SparkafkaDataset.safeDataset(topics.ii, start, end) >>
      SparkafkaDataset.safeDataset(topics.first_topic, start, end) >>
      SparkafkaDataset.safeDataset(topics.second_topic, start, end) >>
      SparkafkaDataset.safeDataset(topics.pencil_topic, start, end)
  }
}
