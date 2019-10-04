package mtest

import java.time.LocalDateTime

import com.github.chenharryhua.nanjin.sparkafka._
import com.github.chenharryhua.nanjin.sparkdb._
import cats.implicits._

class CompileOnlyTest {
  val end   = LocalDateTime.now()
  val start = end.minusYears(1)

  spark.use { implicit s =>
    Sparkafka.safeDataset(topics.ss, start, end) >>
      Sparkafka.safeDataset(topics.si, start, end) >>
      Sparkafka.safeDataset(topics.ii, start, end) >>
      Sparkafka.safeValueDataset(topics.first_topic, start, end) >>
      Sparkafka.safeValueDataset(topics.second_topic, start, end) >>
      Sparkafka.safeValueDataset(topics.pencil_topic, start, end)
  }

  spark.use { implicit s =>
    topics.ss.topicDataset
      .withStartDate(LocalDateTime.now)
      .dateset
      .map(_.deserialized.map(m => topics.ss.decoder(m).decode))
  }
}
