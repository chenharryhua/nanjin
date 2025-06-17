package mtest.kafka.stream

import mtest.kafka.ctx
import org.apache.kafka.streams.kstream.JoinWindows
import org.scalatest.funsuite.AnyFunSuite

import java.time.Duration

class StreamsSerdeTest extends AnyFunSuite {

  test("1.stream-table join") {
    val top = ctx.buildStreams("a1") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a").join(sb.table[Int, Int]("b"))((s, i) => (s, i)).to("c")
    }
    println(top.topology.describe())
  }

  test("2.stream-table left-join") {
    val top = ctx.buildStreams("a2") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a").leftJoin(sb.table[Int, Int]("b"))((s, i) => (s, i)).to("c")
    }
    println(top.topology.describe())
  }

  test("3.group-by-key") {
    val top = ctx.buildStreams("a3") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a").groupByKey.reduce(_ + _).toStream.to("b")
    }
    println(top.topology.describe())
  }

  test("4.foreach") {
    val top = ctx.buildStreams("a4") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a").groupByKey.reduce(_ + _).toStream.foreach((k, v) => println((k, v)))
    }
    println(top.topology.describe())
  }

  test("5.stream-stream join") {
    val top = ctx.buildStreams("a4") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a")
        .join(sb.stream[Int, Int]("b"))(
          (s, i) => s + i.toString,
          JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ZERO))
        .to("c")
    }
    println(top.topology.describe())
  }

  test("6.aggregate stream") {
    val top = ctx.buildStreams("a4") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a").groupByKey.aggregate("")((_, s, k) => s + k).toStream.to("b")
    }
    println(top.topology.describe())
  }

  test("7.count") {
    val top = ctx.buildStreams("a4") { (sb, ss) =>
      import ss.implicits.*
      sb.stream[Int, String]("a").groupByKey.count()
      ()
    }
    println(top.topology.describe())
  }
}
