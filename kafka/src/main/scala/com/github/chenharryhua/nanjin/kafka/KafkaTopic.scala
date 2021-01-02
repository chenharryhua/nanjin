package com.github.chenharryhua.nanjin.kafka

import cats.Show
import cats.effect.{Concurrent, ConcurrentEffect, ContextShift, Resource, Sync, Timer}
import cats.syntax.all._
import com.github.chenharryhua.nanjin.messages.kafka.NJConsumerMessage
import com.github.chenharryhua.nanjin.messages.kafka.codec.KafkaGenericDecoder
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.{RecordContext, TopicNameExtractor}

import scala.util.Try

final class KafkaTopic[F[_], K, V] private[kafka] (val topicDef: TopicDef[K, V], val context: KafkaContext[F])
    extends TopicNameExtractor[K, V] with KafkaTopicSettings[F, K, V] with KafkaTopicProducer[F, K, V]
    with Serializable {
  import topicDef.{serdeOfKey, serdeOfVal}

  val topicName: TopicName = topicDef.topicName

  def withGroupId(gid: String): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context.updateSettings(_.withGroupId(gid)))

  def withTopicName(tn: String): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef.withTopicName(tn), context)

  def withSettings(ks: KafkaSettings): KafkaTopic[F, K, V] =
    new KafkaTopic[F, K, V](topicDef, context.updateSettings(_ => ks))

  def withContext[G[_]](ct: KafkaContext[G]): KafkaTopic[G, K, V] =
    ct.topic(topicDef)

  override def extract(key: K, value: V, rc: RecordContext): String = topicName.value

  //need to reconstruct codec when working in spark
  @transient lazy val codec: KafkaTopicCodec[K, V] = new KafkaTopicCodec(
    serdeOfKey.asKey(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName.value),
    serdeOfVal.asValue(context.settings.schemaRegistrySettings.config).codec(topicDef.topicName.value)
  )

  @inline def decoder[G[_, _]: NJConsumerMessage](cr: G[Array[Byte], Array[Byte]]): KafkaGenericDecoder[G, K, V] =
    new KafkaGenericDecoder[G, K, V](cr, codec.keyCodec, codec.valCodec)

  def record(partition: Int, offset: Long)(implicit sync: Sync[F]): F[Option[ConsumerRecord[Try[K], Try[V]]]] =
    shortLiveConsumer.use(
      _.retrieveRecord(KafkaPartition(partition), KafkaOffset(offset)).map(_.map(decoder(_).tryDecodeKeyValue)))

  override def toString: String = topicName.value

  def show: String = {
    import cats.derived.auto.show._
    s"""
       |topic: $topicName
       |consumer-group-id: ${context.settings.groupId}
       |stream-app-id:     ${context.settings.appId}
       |settings:
       |${context.settings.consumerSettings.show}
       |${context.settings.producerSettings.show}
       |${context.settings.schemaRegistrySettings.show}
       |${context.settings.adminSettings.show}
       |${context.settings.streamSettings.show}
       |
       |${codec.keyCodec.cfg.tag}:
       |${codec.keyCodec.cfg.configProps}
       |${codec.keySchemaFor.schema.toString(true)}
       |
       |${codec.valCodec.cfg.tag}:
       |${codec.valCodec.cfg.configProps}
       |${codec.valSchemaFor.schema.toString(true)}
   """.stripMargin
  }

  // APIs

  def admin(implicit concurrent: Concurrent[F], contextShift: ContextShift[F]): KafkaAdminApi[F] =
    KafkaAdminApi[F, K, V](this)

  def shortLiveConsumer(implicit sync: Sync[F]): Resource[F, ShortLiveConsumer[F]] =
    ShortLiveConsumer(topicName, context.settings.consumerSettings.javaProperties)

  def monitor(implicit
    concurrentEffect: ConcurrentEffect[F],
    timer: Timer[F],
    contextShift: ContextShift[F]): KafkaMonitoringApi[F, K, V] = KafkaMonitoringApi[F, K, V](this)

}

object KafkaTopic {
  implicit def showKafkaTopic[F[_], K, V]: Show[KafkaTopic[F, K, V]] = _.show
}
