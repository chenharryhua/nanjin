package mtest.kafka.stream

import com.github.chenharryhua.nanjin.kafka.{KafkaStateStore, StoreName}
import mtest.kafka.ctx
import org.scalatest.funsuite.AnyFunSuite
import com.github.chenharryhua.nanjin.messages.kafka.codec.SerdeOf
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.state.KeyValueStore

class KafkaStateStoreTest extends AnyFunSuite {
  val table1 = KafkaStateStore[Int, String](StoreName("state.store.test.table.one"))
  val table2 = KafkaStateStore[Int, String](StoreName("state.store.test.table.two"))

  val stream1 = ctx.topic[Int, String]("state.store.test.stream")
  val target  = ctx.topic[Int, String]("table.test.target")

  test("tables") {
    val top = for {
      s1 <- stream1.kafkaStream.kstream
      t1 <- table1.table
      t2 <- table2.table
    } yield s1.join(t1)((s, _) => s).join(t2)((s, _) => s).to(target)

    val describe = ctx.buildStreams(top).topology.describe()
    println(describe)
  }
}
