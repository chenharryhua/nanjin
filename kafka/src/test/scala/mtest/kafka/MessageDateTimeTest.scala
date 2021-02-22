package mtest.kafka

import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.kafka._
import com.sksamuel.avro4s.Encoder
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.circe.generic.JsonCodec
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import java.sql.{Date, Timestamp}
import java.time._

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

  //supported date-time in circe
  @JsonCodec final case class JsonDateTime(
    local: LocalDateTime,
    ld: LocalDate,
    zoned: ZonedDateTime,
    offseted: OffsetDateTime,
    instant: Instant,
    // sqlDate: Date,
    // sqlTs: Timestamp,
    dummy: Int = 0
  )

  //supported date-time in avro4s
  final case class AvroDateTime(
    local: LocalDateTime,
    ld: LocalDate,
    //zoned: ZonedDateTime,
    // offseted: OffsetDateTime ,
    instant: Instant,
    sqlDate: Date,
    sqlTs: Timestamp,
    dummy: Int = 0
  )
  implicitly[Encoder[AvroDateTime]]
}

class MessageDateTimeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  "Message DateTime" - {
    "supported java date-time type" in {
      import DatetimeCase.AllJavaDateTime
      val topic = TopicDef[Int, AllJavaDateTime](TopicName("message.datetime.test")).in(ctx)
      val m     = AllJavaDateTime(LocalDateTime.now, LocalDate.now, Instant.ofEpochMilli(Instant.now.toEpochMilli))
      val data = fs2
        .Stream(ProducerRecords.one(ProducerRecord(topic.topicName.value, 0, m)))
        .through(topic.fs2Channel.producerPipe)

      for {
        _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
        _ <- topic.schemaRegistry.delete
        _ <- data.compile.drain
        r <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
      } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    }
  }
}
