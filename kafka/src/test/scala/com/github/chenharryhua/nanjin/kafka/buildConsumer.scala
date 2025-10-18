package com.github.chenharryhua.nanjin.kafka

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.kafka.TopicName
import fs2.kafka.consumer.MkConsumer
import fs2.kafka.{ConsumerSettings, KafkaByteConsumer}
import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.*

import java.time.Duration
import java.util.OptionalLong
import java.util.regex.Pattern
import java.{lang, util}
import scala.jdk.CollectionConverters.*
import eu.timepit.refined.auto.*
object buildConsumer {
  val topicName: TopicName = TopicName("transient.consumer.topic")

  val tp0 = new TopicPartition(topicName.name.value, 0)
  val tp1 = new TopicPartition(topicName.name.value, 1)
  val tp2 = new TopicPartition(topicName.name.value, 2)

  private val partitionInfos: util.List[PartitionInfo] = List(
    new PartitionInfo(topicName.name.value, 0, null, null, null),
    new PartitionInfo(topicName.name.value, 1, null, null, null),
    new PartitionInfo(topicName.name.value, 2, null, null, null)
  ).asJava

  def apply(
    begin: Map[TopicPartition, java.lang.Long],
    end: Map[TopicPartition, java.lang.Long],
    forTime: Map[TopicPartition, OffsetAndTimestamp]): MkConsumer[IO] = new MkConsumer[IO] {
    override def apply[G[_]](settings: ConsumerSettings[G, ?, ?]): IO[KafkaByteConsumer] = IO(
      new KafkaByteConsumer {

        override def close(option: CloseOptions): Unit = ()

        override def assignment(): util.Set[TopicPartition] = ???
        override def subscription(): util.Set[String] = ???
        override def subscribe(collection: util.Collection[String]): Unit = ()
        override def subscribe(
          collection: util.Collection[String],
          consumerRebalanceListener: ConsumerRebalanceListener): Unit = ()
        override def assign(collection: util.Collection[TopicPartition]): Unit = ()
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
        override def seek(topicPartition: TopicPartition, l: Long): Unit = ()
        override def seek(topicPartition: TopicPartition, offsetAndMetadata: OffsetAndMetadata): Unit = ()
        override def seekToBeginning(collection: util.Collection[TopicPartition]): Unit = ()
        override def seekToEnd(collection: util.Collection[TopicPartition]): Unit = ()
        override def position(topicPartition: TopicPartition): Long = 0
        override def position(topicPartition: TopicPartition, duration: Duration): Long = 0

        override def committed(set: util.Set[TopicPartition]): util.Map[TopicPartition, OffsetAndMetadata] =
          set.asScala.toList.map(tp => tp -> committed(tp)).toMap.asJava
        override def committed(
          set: util.Set[TopicPartition],
          duration: Duration): util.Map[TopicPartition, OffsetAndMetadata] =
          set.asScala.toList.map(tp => tp -> committed(tp, duration)).toMap.asJava

        override def clientInstanceId(duration: Duration): Uuid = ???
        override def metrics(): util.Map[MetricName, ? <: Metric] = ???
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

        def poll(l: Long): ConsumerRecords[Array[Byte], Array[Byte]] = null
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
