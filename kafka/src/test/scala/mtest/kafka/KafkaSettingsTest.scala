package mtest.kafka

import com.github.chenharryhua.nanjin.kafka.config.*
import io.confluent.kafka.serializers.{
  AbstractKafkaSchemaSerDeConfig,
  KafkaAvroDeserializerConfig,
  KafkaAvroSerializerConfig
}
import io.confluent.kafka.serializers.json.{
  KafkaJsonSchemaDeserializerConfig,
  KafkaJsonSchemaSerializerConfig
}
import io.confluent.kafka.serializers.protobuf.{
  KafkaProtobufDeserializerConfig,
  KafkaProtobufSerializerConfig
}
import org.apache.kafka.clients.{admin, consumer, producer, CommonClientConfigs}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.streams.StreamsConfig
import munit.FunSuite

class KafkaSettingsTest extends FunSuite {

  private val BASE_SETTINGS: KafkaSettings =
    KafkaSettings("broker-url:9092", "http://schema-registry")
      .withConsumerProperty(_.AUTO_OFFSET_RESET_CONFIG, "earliest")

  test("1.individual settings builders support literal and typed config keys") {
    val consumer_settings = KafkaConsumerSettings(Map.empty)
      .withProperty("consumer.literal", "a")
      .withProperty(_.AUTO_OFFSET_RESET_CONFIG, "latest")
    val producer_settings = KafkaProducerSettings(Map.empty)
      .withProperty("producer.literal", "b")
      .withProperty(_.ACKS_CONFIG, "all")
    val stream_settings = KafkaStreamSettings(Map.empty)
      .withProperty("stream.literal", "c")
      .withProperty(_.COMMIT_INTERVAL_MS_CONFIG, "100")
    val serde_settings = SerdeSettings(Map.empty)
      .withProperty("serde.literal", "d")
      .withProperty(_.MAX_RETRIES_CONFIG, "5")

    assertEquals(consumer_settings.properties("consumer.literal"), "a")
    assertEquals(consumer_settings.properties(consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest")
    assertEquals(producer_settings.properties("producer.literal"), "b")
    assertEquals(producer_settings.properties(producer.ProducerConfig.ACKS_CONFIG), "all")
    assertEquals(stream_settings.properties("stream.literal"), "c")
    assertEquals(stream_settings.properties(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG), "100")
    assertEquals(serde_settings.properties("serde.literal"), "d")
    assertEquals(serde_settings.properties(AbstractKafkaSchemaSerDeConfig.MAX_RETRIES_CONFIG), "5")
  }

  test("2.broker, SASL, and security settings propagate to every applicable client") {
    val settings = BASE_SETTINGS
      .withBrokers("new-broker:19092")
      .withSaslJaas("jaas-config")
      .withSecurityProtocol(SecurityProtocol.SASL_SSL)

    val client_settings = List(
      settings.consumerSettings.properties,
      settings.producerSettings.properties,
      settings.adminSettings.properties,
      settings.streamSettings.properties)

    client_settings.foreach { properties =>
      assertEquals(properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), "new-broker:19092")
      assertEquals(properties(SaslConfigs.SASL_JAAS_CONFIG), "jaas-config")
      assertEquals(properties(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG), "SASL_SSL")
    }
    assertEquals(
      settings.serdeSettings.properties(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),
      "http://schema-registry")
  }

  test("3.client-specific transformations change only their target settings") {
    val settings = BASE_SETTINGS
      .withProducerProperty(_.CLIENT_ID_CONFIG, "producer")
      .withConsumerProperty(_.CLIENT_ID_CONFIG, "consumer")
      .withStreamingProperty(_.APPLICATION_ID_CONFIG, "streams")
      .withSerdeProperty(_.MAX_RETRIES_CONFIG, "7")
      .withAdminClient(_.withClientId("admin"))

    assertEquals(settings.producerSettings.properties(producer.ProducerConfig.CLIENT_ID_CONFIG), "producer")
    assertEquals(settings.consumerSettings.properties(consumer.ConsumerConfig.CLIENT_ID_CONFIG), "consumer")
    assertEquals(settings.streamSettings.properties(StreamsConfig.APPLICATION_ID_CONFIG), "streams")
    assertEquals(settings.serdeSettings.properties(AbstractKafkaSchemaSerDeConfig.MAX_RETRIES_CONFIG), "7")
    assertEquals(settings.adminSettings.properties(admin.AdminClientConfig.CLIENT_ID_CONFIG), "admin")

    assert(!BASE_SETTINGS.producerSettings.properties.contains(producer.ProducerConfig.CLIENT_ID_CONFIG))
    assert(!BASE_SETTINGS.consumerSettings.properties.contains(consumer.ConsumerConfig.CLIENT_ID_CONFIG))
    assert(!BASE_SETTINGS.streamSettings.properties.contains(StreamsConfig.APPLICATION_ID_CONFIG))
    assert(!BASE_SETTINGS.adminSettings.properties.contains(admin.AdminClientConfig.CLIENT_ID_CONFIG))
  }

  test("4.Avro, JSON, and Protobuf helpers update their format-specific serde keys") {
    val settings = BASE_SETTINGS
      .withAvroSerializerConfig(_.AVRO_REFLECTION_ALLOW_NULL_CONFIG, "true")
      .withAvroDeserializerConfig(_.SPECIFIC_AVRO_READER_CONFIG, "true")
      .withJsonSerializerConfig(_.FAIL_INVALID_SCHEMA, "true")
      .withJsonDeserializerConfig(_.TYPE_PROPERTY, "type")
      .withProtobufSerializerConfig(_.SKIP_KNOWN_TYPES_CONFIG, "true")
      .withProtobufDeserializerConfig(_.DERIVE_TYPE_CONFIG, "true")

    val properties = settings.serdeSettings.properties
    assertEquals(properties(KafkaAvroSerializerConfig.AVRO_REFLECTION_ALLOW_NULL_CONFIG), "true")
    assertEquals(properties(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG), "true")
    assertEquals(properties(KafkaJsonSchemaSerializerConfig.FAIL_INVALID_SCHEMA), "true")
    assertEquals(properties(KafkaJsonSchemaDeserializerConfig.TYPE_PROPERTY), "type")
    assertEquals(properties(KafkaProtobufSerializerConfig.SKIP_KNOWN_TYPES_CONFIG), "true")
    assertEquals(properties(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG), "true")
  }

  test("5.apply and local provide complete defaults, and context retains the settings") {
    val applied = KafkaSettings("host:9092", "http://registry")
    val context = applied.context[cats.effect.IO]

    assertEquals(
      applied.consumerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      "host:9092")
    assertEquals(
      applied.producerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      "host:9092")
    assertEquals(applied.adminSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), "host:9092")
    assertEquals(applied.streamSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), "host:9092")
    assertEquals(
      applied.serdeSettings.properties(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG),
      "http://registry")
    assertEquals(context.settings, applied)
    assertEquals(
      KafkaSettings.local.consumerSettings.properties(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
      "localhost:9092")
  }
}
