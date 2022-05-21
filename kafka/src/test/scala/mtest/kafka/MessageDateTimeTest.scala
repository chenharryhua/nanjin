package mtest.kafka

import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.*
import com.sksamuel.avro4s.Encoder
import fs2.kafka.{ProducerRecord, ProducerRecords}
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

import java.sql.{Date, Timestamp}
import java.time.*

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
    val topic = TopicDef[Int, AllJavaDateTime](TopicName("message.datetime.test")).in(ctx)
    val m     = AllJavaDateTime(LocalDateTime.now, LocalDate.now, Instant.ofEpochMilli(Instant.now.toEpochMilli))
    val data =
      fs2.Stream(ProducerRecords.one(ProducerRecord(topic.topicName.value, 0, m))).through(topic.produce.pipe)
    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence.attempt
      _ <- topic.schemaRegistry.delete
      _ <- data.compile.drain
      r <- topic.shortLiveConsumer.use(_.retrieveLastRecords)
    } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    rst.unsafeRunSync()
  }
}
