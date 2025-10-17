package mtest.kafka.stream

import cats.derived.auto.show.*
import cats.implicits.{catsSyntaxTuple2Semigroupal, showInterpolator}
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import com.github.chenharryhua.nanjin.kafka.AvroTopic
import com.github.chenharryhua.nanjin.kafka.streaming.StreamsSerde
import com.github.chenharryhua.nanjin.messages.kafka.codec.{AvroCodec, AvroFor}
import eu.timepit.refined.auto.*
import mtest.kafka.stream.KafkaStreamingData.{StreamOne, StreamTarget, TableTwo}
import org.apache.kafka.streams.processor.api
import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier, Record}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.state.KeyValueStore
import scala.util.Try

object apps {
  def kafka_streaming(sb: StreamsBuilder, rs: StreamsSerde): Unit = {
    import rs.implicits.*
    implicit val ev: AvroFor[StreamOne] = AvroFor(AvroCodec[StreamOne])
    val a = sb.stream[Int, StreamOne]("stream.test.join.stream.one")
    val b = sb.table[Int, TableTwo]("stream.test.join.table.two")
    a.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to("stream.test.join.target")
  }
  def kafka_streaming_bad_record(sb: StreamsBuilder, rs: StreamsSerde): Unit = {
    import rs.implicits.*
    val tn = TopicName("stream.test.stream.bad.records.one")
    val t2Topic = AvroTopic[Int, TableTwo](TopicName("stream.test.join.table.two"))
    val keyS = rs.keySerde[Int](tn)
    val valS = rs.valueSerde[StreamOne](tn)

    val a = sb.stream[Array[Byte], Array[Byte]](tn.name.value)
    val b = sb.table[Int, TableTwo](t2Topic.topicName.name.value)
    a.flatMap { (k, v) =>
      val r = (
        Try(keyS.deserialize(k)).toOption,
        Try(valS.deserialize(v)).toOption
      ).mapN((_, _))
      println(r)
      r
    }.join(b)((s1, t2) => StreamTarget(s1.name, 0, t2.color))
      .peek((k, v) => println(show"out=($k, $v)"))
      .to("stream.test.join.target")
  }

  def transformer_app(sb: StreamsBuilder, ksb: StreamsSerde): Unit = {
    val topic1 = TopicName("stream.builder.test.stream1")
    val topic2 = TopicName("stream.builder.test.table2")
    val tgt = TopicName("stream.builder.test.target")
    val store = TopicName("stream.builder.test.store")
    import ksb.implicits.*
    sb.addStateStore(
      ksb.store[Int, String]("stream.builder.test.store").inMemoryKeyValueStore.keyValueStoreBuilder)

    val processor: ProcessorSupplier[Int, String, Int, String] =
      new ProcessorSupplier[Int, String, Int, String] {
        var kvStore: KeyValueStore[Int, String] = _
        var ctx: api.ProcessorContext[Int, String] = _
        override def get(): Processor[Int, String, Int, String] = new Processor[Int, String, Int, String] {
          override def init(context: api.ProcessorContext[Int, String]): Unit = {
            kvStore = context.getStateStore[KeyValueStore[Int, String]](store.name.value)
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

    sb.stream[Int, String](topic1.name.value)
      .process(processor, store.name.value)
      .join(sb.table[Int, String](topic2.name.value))(_ + _)
      .to(tgt.name.value)
  }

}
