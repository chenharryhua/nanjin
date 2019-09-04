package mtest

import org.scalatest.funsuite.AnyFunSuite
import org.typelevel.discipline.scalatest.Discipline
import cats.laws.discipline.InvariantTests
import com.github.chenharryhua.nanjin.codec.KafkaCodec

class InvariantTest extends AnyFunSuite with Discipline {

 // checkAll("KafkaCodec", InvariantTests[KafkaCodec].invariant[Int, Int, Int])

}
