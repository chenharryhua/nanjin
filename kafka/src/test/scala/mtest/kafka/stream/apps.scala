package mtest.kafka.stream

import cats.derived.auto.show.*
import cats.implicits.{catsSyntaxTuple2Semigroupal, showInterpolator}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.streaming.StreamsSerde
import eu.timepit.refined.auto.*
import mtest.kafka.stream.KafkaStreamingData.{StreamOne, StreamTarget, TableTwo}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier, Record}
import org.apache.kafka.streams.scala.ImplicitConversions.*
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes.{byteArraySerde, intSerde, stringSerde}
import org.apache.kafka.streams.state.KeyValueStore

import scala.util.Try

object apps {
  def kafka_streaming(sb: StreamsBuilder, rs: StreamsSerde): Unit = {
    implicit val ev1: Serde[StreamTarget] = rs.asValue[StreamTarget]
    implicit val ev2: Serde[StreamOne] = rs.asValue[StreamOne]
    implicit val ev3: Serde[TableTwo] = rs.asValue[TableTwo]
    val a = sb.stream[Int, StreamOne]("stream.test.join.stream.one")
    val b = sb.table[Int, TableTwo]("stream.test.join.table.two")
    a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to("stream.test.join.target")
  }
  def kafka_streaming_bad_record(sb: StreamsBuilder, rs: StreamsSerde): Unit = {
    val tn = TopicName("stream.test.stream.bad.records.one")
    val t2Topic = TopicDef[Int, TableTwo](TopicName("stream.test.join.table.two"))

    implicit val ev1: Serde[StreamTarget] = rs.asValue[StreamTarget]
    implicit val ev2: Serde[StreamOne] = rs.asValue[StreamOne]
    implicit val ev3: Serde[TableTwo] = rs.asValue[TableTwo]

    val a = sb.stream[Array[Byte], Array[Byte]](tn.value)
    val b = sb.table[Int, TableTwo](t2Topic.topicName.value)
    a.flatMap { (k, v) =>
      val r = (
        Try(rs.asKey[Int].deserializer().deserialize(tn.value, k)).toOption,
        Try(rs.asValue[StreamOne].deserializer().deserialize(tn.value, v)).toOption
      ).mapN((_, _))
      println(r)
      r
    }.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to("stream.test.join.target")
  }

  def transformer_app(sbb: StreamsBuilder, ksb: StreamsSerde): Unit = {
    val topic1 = TopicName("stream.builder.test.stream1")
    val topic2 = TopicName("stream.builder.test.table2")
    val tgt = TopicName("stream.builder.test.target")
    val store = TopicName("stream.builder.test.store")
    val sb: StreamsBuilder = new StreamsBuilder(
      sbb.addStateStore(
        ksb
          .store[Int, String](TopicName("stream.builder.test.store"))
          .inMemoryKeyValueStore
          .keyValueStoreBuilder))

    val processor: ProcessorSupplier[Int, String, Int, String] =
      new ProcessorSupplier[Int, String, Int, String] {
        var kvStore: KeyValueStore[Int, String] = _
        var ctx: api.ProcessorContext[Int, String] = _
        override def get(): Processor[Int, String, Int, String] = new Processor[Int, String, Int, String] {
          override def init(context: api.ProcessorContext[Int, String]): Unit = {
            kvStore = context.getStateStore[KeyValueStore[Int, String]](store.value)
            ctx = context
            println("transformer initialized")
          }

          override def close(): Unit =
            // kvStore.close()
            println("transformer closed")

          override def process(record: Record[Int, String]): Unit = {
            println(record.toString)
            kvStore.put(record.key(), record.value())
            ctx.forward(record)
          }
        }
      }

    sb.stream[Int, String](topic1.value)
      .process(processor, store.value)
      .join(sb.table[Int, String](topic2.value))(_ + _)
      .to(tgt.value)
  }

}
