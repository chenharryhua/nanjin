package mtest.kafka

import java.time._

import cats.derived.auto.show._
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka._
import com.github.chenharryhua.nanjin.datetime._
import com.sksamuel.avro4s.Encoder
import io.circe.generic.JsonCodec
import io.circe.generic.auto._
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
  @JsonCodec final case class AllJavaDateTime2(
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
  final case class AllJavaDateTime3(
    local: LocalDateTime,
    ld: LocalDate,
    // zoned: ZonedDateTime ,
    // offseted: OffsetDateTime ,
    instant: Instant,
    // sqlDate: Date,
    // sqlTs: Timestamp,
    dummy: Int = 0
  )
  implicitly[Encoder[AllJavaDateTime3]]

}

class MessageDateTimeTest extends AnyFunSuite {

  test("supported java date-time type") {
    import DatetimeCase.AllJavaDateTime
    val topic = TopicDef[Int, AllJavaDateTime]("message.datetime.test").in(ctx)
    val m     = AllJavaDateTime(LocalDateTime.now, LocalDate.now, Instant.now())
    val rst = for {
      _ <- topic.admin.idefinitelyWantToDeleteTheTopic
      _ <- topic.schemaRegistry.delete
      _ <- topic.send(0, m)
      r <- topic.consumerResource.use(_.retrieveLastRecords)
    } yield assert(topic.decoder(r.head).decodeValue.value() === m)
    rst.unsafeRunSync()
  }
}
