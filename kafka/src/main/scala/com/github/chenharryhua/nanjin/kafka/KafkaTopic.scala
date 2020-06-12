package com.github.chenharryhua.nanjin.kafka

import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.implicits._
import com.github.chenharryhua.nanjin.kafka.codec._
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

final class KafkaTopic[F[_], K, V] private[kafka] (
  val topicDef: TopicDef[K, V],
  val settings: KafkaSettings)
    extends TopicNameExtractor[K, V] with KafkaTopicSettings[F, K, V]
    with KafkaTopicProducer[F, K, V] with Serializable {
  import topicDef.{serdeOfKey, serdeOfVal}

  val topicName: TopicName = topicDef.topicName

  def withGroupId(gid: String): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, settings.withGroupId(gid))

  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value

  //need to reconstruct codec when working in spark
  @transient lazy val codec: KafkaTopicCodec[K, V] = new KafkaTopicCodec(
    serdeOfKey.asKey(settings.schemaRegistrySettings.config).codec(topicDef.topicName),
    serdeOfVal.asValue(settings.schemaRegistrySettings.config).codec(topicDef.topicName)
  )

  def decoder[G[_, _]: NJConsumerMessage](
    cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  override def toString: String = {
    import cats.derived.auto.show._
    s"""
       |topic: $topicName
       |consumer-group-id: ${settings.groupId}
       |stream-app-id:     ${settings.appId}
       |settings:
       |${settings.consumerSettings.show}
       |${settings.producerSettings.show}
       |${settings.schemaRegistrySettings.show}
       |${settings.adminSettings.show}
       |${settings.streamSettings.show}
       |
       |${codec.keySerde.tag}:
       |${codec.keySerde.configProps}
       |${codec.keySchemaFor.schema.toString(true)}
       |
       |${codec.valSerde.tag}:
       |${codec.valSerde.configProps}
       |${codec.valSchemaFor.schema.toString(true)}
   """.stripMargin
  }

  // APIs
  def schemaRegistry(implicit sync: Sync[F]): KafkaSchemaRegistryApi[F] =
    KafkaSchemaRegistryApi[F](this)

  def admin(implicit concurrent: Concurrent[F], contextShift: ContextShift[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLivedConsumer(implicit sync: Sync[F]): Resource[F, ShortLivedConsumer[F]] =
    ShortLivedConsumer(topicName, settings.consumerSettings.javaProperties)

  def monitor(implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): KafkaMonitoringApi[F, K, V] = KafkaMonitoringApi[F, K, V](this)
}
