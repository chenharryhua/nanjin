package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.TopicName
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.{ConsumerSettings, KafkaByteConsumer}
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.*

import java.time.Duration
import java.util.OptionalLong
import java.util.concurrent.{atomic, ConcurrentHashMap}
import java.util.regex.Pattern
import java.{lang, util}
import scala.jdk.CollectionConverters.*
object buildConsumer {
  val topicName: TopicName = TopicName("transient.consumer.topic")

  val tp0 = new TopicPartition(topicName.value, 0)
  val tp1 = new TopicPartition(topicName.value, 1)
  val tp2 = new TopicPartition(topicName.value, 2)

  private val partitionInfos: util.List[PartitionInfo] = List(
    new PartitionInfo(topicName.value, 0, null, null, null),
    new PartitionInfo(topicName.value, 1, null, null, null),
    new PartitionInfo(topicName.value, 2, null, null, null)
  ).asJava

  def apply(
    begin: Map[TopicPartition, java.lang.Long],
    end: Map[TopicPartition, java.lang.Long],
    forTime: Map[TopicPartition, OffsetAndTimestamp],
    records: Map[(TopicPartition, Long), List[ConsumerRecord[Array[Byte], Array[Byte]]]] = Map.empty,
    consumerMetrics: Map[MetricName, Metric] = Map.empty): MkConsumer[IO] = new MkConsumer[IO] {
    override def apply[G[_]](settings: ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] = IO(
      new KafkaByteConsumer {

        private val assigned = new atomic.AtomicReference[List[TopicPartition]](Nil)
        private val positions = new ConcurrentHashMap[TopicPartition, java.lang.Long]()

        override def close(option: CloseOptions): Unit = ()

        override def assignment(): util.Set[TopicPartition] = assigned.get().toSet.asJava
        override def subscription(): util.Set[String] = ???
        override def subscribe(collection: util.Collection[String]): Unit = ()
        override def subscribe(
          collection: util.Collection[String],
          consumerRebalanceListener: ConsumerRebalanceListener): Unit = ()
        override def assign(collection: util.Collection[TopicPartition]): Unit =
          assigned.set(collection.asScala.toList)
        override def subscribe(pattern: Pattern, consumerRebalanceListener: ConsumerRebalanceListener): Unit =
          ()
        override def subscribe(pattern: Pattern): Unit = ()
        override def unsubscribe(): Unit = ()
        override def poll(duration: Duration): ConsumerRecords[Array[Byte], Array[Byte]] =
          poll(duration.toMillis)
        override def commitSync(): Unit = ()
        override def commitSync(duration: Duration): Unit = ()
        override def commitSync(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = ()
        override def commitSync(map: util.Map[TopicPartition, OffsetAndMetadata], duration: Duration): Unit =
          ()
        override def commitAsync(): Unit = ()
        override def commitAsync(offsetCommitCallback: OffsetCommitCallback): Unit = ()
        override def commitAsync(
          map: util.Map[TopicPartition, OffsetAndMetadata],
          offsetCommitCallback: OffsetCommitCallback): Unit = ()
        override def seek(topicPartition: TopicPartition, l: Long): Unit = {
          val _ = positions.put(topicPartition, java.lang.Long.valueOf(l))
        }
        override def seek(topicPartition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Unit = ()
        override def seekToBeginning(collection: util.Collection[TopicPartition]): Unit = ()
        override def seekToEnd(collection: util.Collection[TopicPartition]): Unit = ()
        override def position(topicPartition: TopicPartition): Long =
          Option(positions.get(topicPartition)).fold(0L)(_.longValue)
        override def position(topicPartition: TopicPartition, duration: Duration): Long =
          position(topicPartition)

        override def committed(set: util.Set[TopicPartition]): util.Map[TopicPartition, OffsetAndMetadata] =
          set.asScala.toList.map(tp => tp -> committed(tp)).toMap.asJava
        override def committed(
          set: util.Set[TopicPartition],
          duration: Duration): util.Map[TopicPartition, OffsetAndMetadata] =
          set.asScala.toList.map(tp => tp -> committed(tp, duration)).toMap.asJava

        override def clientInstanceId(duration: Duration): Uuid = ???
        override def metrics(): util.Map[MetricName, ? <: Metric] = consumerMetrics.asJava
        override def partitionsFor(s: String): util.List[PartitionInfo] = partitionInfos
        override def partitionsFor(s: String, duration: Duration): util.List[PartitionInfo] =
          partitionInfos
        override def listTopics(): util.Map[String, util.List[PartitionInfo]] = ???
        override def listTopics(duration: Duration): util.Map[String, util.List[PartitionInfo]] = ???
        override def paused(): util.Set[TopicPartition] = ???
        override def pause(collection: util.Collection[TopicPartition]): Unit = ()
        override def resume(collection: util.Collection[TopicPartition]): Unit = ()
        override def offsetsForTimes(
          map: util.Map[TopicPartition, lang.Long]): util.Map[TopicPartition, OffsetAndTimestamp] =
          forTime.asJava
        override def offsetsForTimes(
          map: util.Map[TopicPartition, lang.Long],
          duration: Duration): util.Map[TopicPartition, OffsetAndTimestamp] = forTime.asJava
        override def beginningOffsets(
          collection: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] =
          begin.asJava
        override def beginningOffsets(
          collection: util.Collection[TopicPartition],
          duration: Duration): util.Map[TopicPartition, lang.Long] =
          begin.asJava
        override def endOffsets(
          collection: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] =
          end.asJava
        override def endOffsets(
          collection: util.Collection[TopicPartition],
          duration: Duration): util.Map[TopicPartition, lang.Long] =
          end.asJava
        override def currentLag(topicPartition: TopicPartition): OptionalLong = ???
        override def groupMetadata(): ConsumerGroupMetadata = ???
        override def enforceRebalance(): Unit = ()
        override def enforceRebalance(s: String): Unit = ()
        override def close(): Unit = ()
        override def close(duration: Duration): Unit = ()
        override def wakeup(): Unit = ()

        def poll(_l: Long): ConsumerRecords[Array[Byte], Array[Byte]] = {
          val _ = _l
          val selected: Map[TopicPartition, util.List[ConsumerRecord[Array[Byte], Array[Byte]]]] =
            assigned.get().flatMap { tp =>
              Option(positions.get(tp)).flatMap(offset =>
                records.get(tp -> offset.longValue).map(tp -> _.asJava))
            }.toMap
          new ConsumerRecords(selected.asJava, Map.empty[TopicPartition, OffsetAndMetadata].asJava)
        }
        def committed(topicPartition: TopicPartition): OffsetAndMetadata = null
        def committed(topicPartition: TopicPartition, duration: Duration): OffsetAndMetadata = null
        def registerMetricForSubscription(metric: org.apache.kafka.common.metrics.KafkaMetric): Unit = ???
        def subscribe(pattern: org.apache.kafka.clients.consumer.SubscriptionPattern): Unit = ???
        def subscribe(
          pattern: org.apache.kafka.clients.consumer.SubscriptionPattern,
          callback: org.apache.kafka.clients.consumer.ConsumerRebalanceListener): Unit = ???
        def unregisterMetricFromSubscription(metric: org.apache.kafka.common.metrics.KafkaMetric): Unit = ???

      })
  }
}
