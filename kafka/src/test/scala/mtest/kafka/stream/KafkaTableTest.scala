package mtest.kafka.stream

import com.github.chenharryhua.nanjin.kafka.{ StoreName}
import mtest.kafka.ctx
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._

class KafkaTableTest extends AnyFunSuite {
  val table1  = ctx.topic[Int, String]("table.test.table.one")
  val table2  = ctx.topic[Int, String]("table.test.table.two")
  val stream3 = ctx.topic[Int, String]("table.test.stream.two")
  val global4 = ctx.topic[Int, String]("table.test.global.three")

  val target = ctx.topic[Int, String]("table.test.target")

  test("tables") {
    val top = for {
      s3 <- stream3.kafkaStream.kstream
      g4 <- global4.kafkaStream.gktable
    } yield s3.join(g4)((k, _) => k, (v, _) => v).to(target) //.join(t1)((s, _) => s).join(t2)((s, _) => s).to(target)

    val describe = ctx.buildStreams(top).topology.describe()
    println(describe)

  }
}
