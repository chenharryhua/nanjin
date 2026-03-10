package mtest.kafka

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.*
import com.sksamuel.avro4s.Encoder
import fs2.kafka.ProducerRecord
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.*
import fs2.Chunk
import org.apache.kafka.clients.producer.RecordMetadata
import io.circe.Codec

object DatetimeCase {

  final case class AllJavaDateTime(
    local: LocalDateTime,
    ld: LocalDate,
    // zoned: ZonedDateTime,
    // offseted: OffsetDateTime,
    instant: Instant,
    // sqlDate: Date,
    // sqlTs: Timestamp,
    dummy: Int = 0
  )

  // supported date-time in circe
  final case class JsonDateTime(
    local: LocalDateTime,
    ld: LocalDate,
    zoned: ZonedDateTime,
    offseted: OffsetDateTime,
    instant: Instant,
    // sqlDate: Date,
    // sqlTs: Timestamp,
    dummy: Int = 0
  ) derives Codec.AsObject

  // supported date-time in avro4s
  final case class AvroDateTime(
    local: LocalDateTime,
    ld: LocalDate,
    // zoned: ZonedDateTime,
    // offseted: OffsetDateTime ,
    instant: Instant,
    sqlDate: Date,
    sqlTs: Timestamp,
    dummy: Int = 0
  )
  implicitly[Encoder[AvroDateTime]]
}

class MessageDateTimeTest extends AnyFunSuite {

  test("supported java date-time type") {
    import DatetimeCase.AllJavaDateTime
    val topic = AvroTopic[Integer, AllJavaDateTime](TopicName("message.datetime.test"))
    val m = AllJavaDateTime(LocalDateTime.now, LocalDate.now, Instant.ofEpochMilli(Instant.now.toEpochMilli))
    val data: fs2.Stream[IO, Chunk[RecordMetadata]] =
      fs2
        .Stream(ProducerRecord(topic.topicName.value, Integer.valueOf(0), m))
        .through(ctx.sharedProduce[Integer, AllJavaDateTime](topic.pair).sink)
    val rst = for {
      _ <- ctx
        .admin(topic.topicName)
        .use(_.iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt)
      _ <- data.compile.drain
    } yield ()
    rst.unsafeRunSync()
  }
}
