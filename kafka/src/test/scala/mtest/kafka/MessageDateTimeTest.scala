package mtest.kafka

import java.sql.{Date, Timestamp}
import java.time._

import com.github.chenharryhua.nanjin.kafka._
import com.sksamuel.avro4s.Encoder
import io.circe.generic.JsonCodec
import org.scalatest.funsuite.AnyFunSuite

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

class MessageDateTimeTest extends AnyFunSuite {

  test("supported java date-time type") {
    import DatetimeCase.AllJavaDateTime
    val topic = TopicDef[Int, AllJavaDateTime](TopicName("message.datetime.test")).in(ctx)
    val m = AllJavaDateTime(
      LocalDateTime.now,
      LocalDate.now,
      Instant.ofEpochMilli(Instant.now.toEpochMilli))
    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence
      _ <- topic.schemaRegistry.delete
      _ <- topic.send(0, m)
      r <- topic.shortLivedConsumer.use(_.retrieveLastRecords)
    } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    rst.unsafeRunSync()
  }
}
