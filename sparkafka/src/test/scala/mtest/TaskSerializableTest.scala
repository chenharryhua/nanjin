package mtest

import java.time.{LocalDate, LocalDateTime}

import cats.effect.IO
import cats.implicits._
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.sparkafka._
import frameless.cats.implicits._
import cats.derived.auto.show._
class TaskSerializableTest extends AnyFunSuite {

  val e     = EmbeddedForTaskSerializable(0, LocalDateTime.now)
  val data  = ForTaskSerializable(0, "a", LocalDate.now, LocalDateTime.now, e)
  val topic = topics.serializableTopic
  (topic.schemaRegistry.register >>
    topic.producer.send(0, data) >>
    topic.producer.send(1, data)).unsafeRunSync()

  test("serializable task will not throw exception") {
    spark.use { s =>
      s.datasetFromKafka(topics.serializableTopic).flatMap(_.consumerRecords.show[IO]())
    }.unsafeRunSync
  }
}
