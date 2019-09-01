package mtest

import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import cats.implicits._
import cats.Eq
import cats.laws.discipline.BitraverseTests
import com.github.chenharryhua.nanjin.codec.BitraverseKafkaRecord
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalacheck.Arbitrary
import org.scalacheck.ScalacheckShapeless._

class MessageBitraverseTest extends AnyFunSuite with Discipline with BitraverseKafkaRecord {
  /*  implicit val cr: Arbitrary[ConsumerRecord[Int, Int]] = null

  checkAll(
    "ConsumerRecord.Bitraverse",
    BitraverseTests[ConsumerRecord].bitraverse[List, Int, Int, Int, Int, Int, Int])
 */
}
