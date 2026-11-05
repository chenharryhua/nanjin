# Kafka module guide

The Kafka module provides one immutable `KafkaContext` for typed production and consumption,
Confluent Schema Registry integration, Kafka Streams topologies, administrative operations, and
serialization-friendly record models. It builds on fs2-kafka and the Apache Kafka clients while
adding Avro, JSON Schema, Protobuf, and Circe Serdes.

## Contents

- [Dependency and services](#dependency-and-services)
- [Configuration](#configuration)
  - [Create a context](#create-a-context)
  - [Configuration scope](#configuration-scope)
- [Topics and Serdes](#topics-and-serdes)
  - [Define a topic](#define-a-topic)
  - [Serde families](#serde-families)
  - [Transform application types](#transform-application-types)
  - [Schema Registry](#schema-registry)
- [Produce records](#produce-records)
- [Consume records](#consume-records)
  - [Subscription and commit](#subscription-and-commit)
  - [Consumption modes](#consumption-modes)
  - [Deserialization failures](#deserialization-failures)
- [Generic Avro records](#generic-avro-records)
- [Record model](#record-model)
- [Kafka Streams](#kafka-streams)
  - [Run a topology](#run-a-topology)
  - [State stores](#state-stores)
- [Administration](#administration)
- [Wire format and compatibility](#wire-format-and-compatibility)
- [Resource and operational guidance](#resource-and-operational-guidance)
- [Source map and validation](#source-map-and-validation)

## Dependency and services

Add the Kafka module:

```scala
resolvers += "Confluent Maven Repo".at("https://packages.confluent.io/maven/")

libraryDependencies += "com.github.chenharryhua" %% "nj-kafka" % "<version>"
```

Most runtime operations require:

- Kafka brokers for producers, consumers, Streams, and admin calls.
- Confluent Schema Registry for Avro, JSON Schema, and Protobuf Serdes.

`KafkaSettings.local` points to `localhost:9092` and `http://localhost:8081` using `PLAINTEXT`.
Primitive Serdes and plain Circe JSON do not access Schema Registry themselves, but context methods
that materialize a `TopicDef`, `StreamsSerde`, or `TopicSchemaRegistry` still require the registry URL
to be configured. `consumeBytes` and the explicit serializer/deserializer overloads bypass Schema
Registry entirely.

## Configuration

### Create a context

`KafkaSettings` separates consumer, producer, admin, Streams, and Serde properties. Its convenience
constructor configures brokers, Schema Registry, and `PLAINTEXT` for every Kafka client family:

```scala
import cats.effect.IO
import com.github.chenharryhua.nanjin.kafka.KafkaContext
import com.github.chenharryhua.nanjin.kafka.config.KafkaSettings
import org.apache.kafka.common.security.auth.SecurityProtocol

val settings =
  KafkaSettings(
    brokers = "kafka-1.example.com:9093,kafka-2.example.com:9093",
    schemaRegistry = "https://schema-registry.example.com"
  )
    .withSecurityProtocol(SecurityProtocol.SASL_SSL)
    .withSaslJaas(sys.env("KAFKA_SASL_JAAS_CONFIG"))
    .withConsumerProperty(_.CLIENT_ID_CONFIG, "orders-consumer")
    .withProducerProperty(_.CLIENT_ID_CONFIG, "orders-producer")
    .withStreamingProperty(_.STATE_DIR_CONFIG, "/var/lib/orders-streams")
    .withSerdeProperty(_.AUTO_REGISTER_SCHEMAS, "false")

val kafka: KafkaContext[IO] = settings.context[IO]
```

`withBrokers`, `withSaslJaas`, and `withSecurityProtocol` update the consumer, producer, admin, and
Streams settings together. Schema Registry settings remain separate. Configure any additional Kafka
SASL and TLS properties required by the deployment. The context's cached Schema Registry client is
currently constructed without authentication or TLS properties from `serdeSettings`; setting those
properties does not configure that client's connection to a secured registry.

### Configuration scope

| Method | Target |
| --- | --- |
| `withBrokers` | Consumer, producer, admin, and Streams bootstrap servers |
| `withSaslJaas` | Consumer, producer, admin, and Streams JAAS configuration |
| `withSecurityProtocol` | Consumer, producer, admin, and Streams security protocol |
| `withConsumerProperty` | Kafka consumer properties |
| `withProducerProperty` | Kafka producer properties |
| `withStreamingProperty` | Kafka Streams properties |
| `withSerdeProperty` | Shared Confluent Serde properties |
| `withAvroSerializerConfig` / `withAvroDeserializerConfig` | Avro-specific Serde properties |
| `withJsonSerializerConfig` / `withJsonDeserializerConfig` | JSON Schema-specific Serde properties |
| `withProtobufSerializerConfig` / `withProtobufDeserializerConfig` | Protobuf-specific Serde properties |
| `withAdminClient` | fs2-kafka `AdminClientSettings` |

All transformations are immutable. `KafkaContext.updateConfig` returns a new context and leaves the
original settings unchanged:

```scala
val replayKafka = kafka.updateConfig(
  _.withConsumerProperty(_.CLIENT_ID_CONFIG, "orders-replay")
)
```

The context is cheap to keep. It creates its cached Schema Registry client lazily on the first
operation that requires it and shares that client across operations from the same context.

## Topics and Serdes

### Define a topic

A `TopicDef[K, V]` binds one topic name to unregistered key and value Serdes:

```scala
import com.github.chenharryhua.nanjin.kafka.TopicDef
import com.github.chenharryhua.nanjin.kafka.serdes.Primitive

val auditTopic: TopicDef[String, String] =
  TopicDef(
    topicName = "audit-events",
    key = Primitive[String],
    value = Primitive[String]
  )
```

Passing a `TopicDef` to `kafka.consume`, `kafka.produce`, `kafka.serde`, or `kafka.store` materializes
and configures each side in key or value mode. The resulting `TopicSerde` is bound to the topic name,
which matters for Confluent subject naming.

### Serde families

| Family | Kafka-side type | Schema Registry | Wire representation |
| --- | --- | --- | --- |
| `Primitive` | `String`, `Array[Byte]`, boxed numeric/boolean types, `ByteBuffer`, `UUID` | No | Kafka built-in serializer format |
| `Structured[GenericRecord]` | Avro `GenericRecord` | Yes | Confluent envelope and Avro payload |
| `Structured[JsonNode]` | Jackson `JsonNode` | Yes | Confluent envelope and JSON payload |
| `Structured[DynamicMessage]` | Protobuf `DynamicMessage` | Yes | Confluent Protobuf wire format |
| `Structured[Json]` | Circe `Json` | No | Compact UTF-8 JSON without a Confluent envelope |

Kafka's primitive Serdes use boxed Java types such as `java.lang.Integer`. `BiTransform` provides
bridges to optional Scala primitives when needed.

### Transform application types

Every `Unregistered[A]` supports:

- `emap(to)(from)` for an explicit bidirectional conversion.
- `become[B]` using a `BiTransform[A, B]`.
- `option` and `orNull` for Kafka tombstone handling.

For example, a Circe-coded domain value can use the plain JSON Serde:

```scala
import com.github.chenharryhua.nanjin.kafka.serdes.Structured
import io.circe.{Codec, Json}

final case class AuditEvent(action: String, actor: String) derives Codec.AsObject

val events: TopicDef[String, AuditEvent] =
  TopicDef(
    "audit-events-json",
    Primitive[String],
    Structured[Json].become[AuditEvent]
  )
```

Structured application mappings include:

- Avro4s `SchemaFor`, `Encoder`, and `Decoder` through `KafkaCodec.avro`.
- Circe `Encoder` and `Decoder` for plain JSON.
- ScalaPB generated messages through `KafkaCodec.protobuf`.
- Jackson `ObjectMapper` mappings through `KafkaCodec.json`.
- Existing Chimney or Monocle isomorphisms.

`BiTransform` does not promise byte-for-byte or object-for-object round trips. Codec-backed mappings
may fill defaults, reorder fields, add envelopes, or discard unknown fields.

### Schema Registry

`kafka.schemaRegistry(topicName)` returns a topic-scoped `TopicSchemaRegistry`. Subjects use
Confluent's standard names `<topic>-key` and `<topic>-value`.

```scala
import com.github.chenharryhua.nanjin.kafka.serdes.KafkaCodec

val registry = kafka.schemaRegistry("audit-events-avro")
val eventSchema = KafkaCodec.avro[AuditEvent].schema

val registerValueSchema =
  registry.register(value = Some(eventSchema))

val fetchSchemas = registry.fetchOptionalAvroSchema
```

The API can register `ParsedSchema` values, fetch Avro, JSON Schema, or Protobuf pairs, and delete both
subjects. Optional fetch methods return `None` when a subject is absent or has another schema type.
`fetchAvroSchema` requires both key and value Avro subjects.

Registry calls use the blocking pool because Confluent's client is synchronous. Deleting subjects is
destructive. A failed deletion, including an absent subject, is represented by an empty
deleted-version list for that side rather than raised to the caller.

## Produce records

`kafka.produce(topic)` returns a typed `ProduceKafka`. One-shot methods open and close a producer for
each call:

```scala
import fs2.Stream

val producer = kafka.produce(auditTopic)

val one = producer.produceOne("order-123", "created")

val batch = producer.produce(
  List(
    "order-124" -> "created",
    "order-125" -> "canceled"
  )
)

val stream =
  Stream
    .emits(List("order-126" -> "created", "order-127" -> "created"))
    .covary[IO]
    .through(producer.pairSink)
    .compile
    .drain
```

Producer entry points:

| API | Input | Lifecycle |
| --- | --- | --- |
| `clientR` | Direct `KafkaProducer` use | `Resource`-scoped producer |
| `clientS` | Direct `KafkaProducer` use | Stream-scoped producer |
| `pairSink` | `(K, V)` pairs | One producer for the stream; targets the configured topic |
| `sink` | fs2-kafka `ProducerRecord[K, V]` | Preserves explicit topic, partition, timestamp, and headers |
| `chunkSink` | `ProducerRecords[K, V]` | Sends each supplied batch |
| `produceOne` | Pair or `ProducerRecord` | Short-lived producer and one metadata result |
| `produce` | Any `Foldable` of pairs | Short-lived producer and one `ProducerResult` |

Prefer `pairSink`, `sink`, `chunkSink`, or an explicitly shared `clientR` for sustained production.
Repeated `produceOne` calls repeatedly acquire producers and are intended for testing, REPL use, or
infrequent sends.

## Consume records

### Subscription and commit

A subscribing consumer needs a group id. The returned stream emits fs2-kafka
`CommittableConsumerRecord` values:

```scala
import cats.syntax.all.*
import fs2.kafka.AutoOffsetReset

val consumer =
  kafka
    .consume(auditTopic)
    .updateConfig(
      _.withGroupId("audit-indexer")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
    )

val consume: IO[Unit] =
  consumer.subscribe.evalMap { record =>
    IO.println(s"${record.record.key}: ${record.record.value}") >>
      record.offset.commit
  }.compile.drain
```

Commit only after downstream handling succeeds. For higher throughput, collect offsets and use
`connector.commitBatch(n, duration)`:

```scala
import com.github.chenharryhua.nanjin.kafka.connector.commitBatch

import scala.concurrent.duration.*

val consumeInBatches: IO[Unit] =
  consumer.subscribe
    .evalTap(record => IO.println(record.record.value))
    .map(_.offset)
    .through(commitBatch(n = 100, d = 5.seconds))
    .compile
    .drain
```

Place batching before downstream truncation or cancellation so the consumer remains open while the
pending commit executes.

### Consumption modes

| Mode | Assignment | Commit behavior | Completion |
| --- | --- | --- | --- |
| `subscribe` | Consumer-group subscription with rebalancing | Controlled by consumer settings; records also expose committable offsets | Unbounded |
| `partitionsMapStream` | Consumer-group subscription | Same settings, with one stream per partition | Unbounded |
| `assign` | Manual assignment of every topic partition | No group rebalancing | Unbounded |
| `assign(Map(partition -> offset))` | Manual assignment and explicit seek | Uses configured auto-commit; forces `AutoOffsetReset.None` | Unbounded |
| `assign(Instant)` | Manual assignment at the first offset at or after a timestamp | Uses configured consumer settings | Unbounded |
| `manualCommitStream` | Consumer-group subscription with one stream per partition | Auto-commit disabled; exposes `commitSync` and `commitAsync` | Unbounded |
| `circumscribedStream` | Manual assignment to resolved offset ranges | Auto-commit disabled | Record-driven stop; see the boundary caveat below |

Each partition stream stops after emitting the first record whose offset is at least `until - 1`.
If that terminal offset is missing (for example, after compaction), it can emit a record outside
`[from, until)` or remain open waiting for a later record. Completion is not guaranteed solely by
reaching the broker's end offset.

An empty explicit map passed to `assign(Map.empty)` raises `EmptyTopicPartitionMap`. An empty range
passed to `circumscribedStream` produces no bounded stream. Time assignment seeks a partition to its
end when Kafka has no offset at or after the requested instant.

`CircumscribedStream.stream` merges the partition streams and invokes `stopConsuming` on finalization.
Already-fetched records can still drain after an explicit stop request.

### Deserialization failures

Normal `consume(topic)` lets key or value deserialization failures fail the consumer stream.
`attemptConsume(topic)` changes each side independently to `Either[Throwable, A]`:

```scala
val tolerant = kafka.attemptConsume(events)

val inspectFailures: IO[Unit] =
  tolerant
    .updateConfig(_.withGroupId("audit-validation"))
    .subscribe
    .evalMap { committable =>
      val keyResult = committable.record.key
      val valueResult = committable.record.value
      IO.println(s"key=$keyResult value=$valueResult")
    }
    .compile
    .drain
```

A bad key does not discard a successfully decoded value, and vice versa. This protection applies to
Serde decoding only; broker errors, user effects, and other stream failures still fail the stream.

## Generic Avro records

The generic connector reads and writes raw Kafka bytes around Avro `GenericRecord` values shaped as
`NJConsumerRecord`. Both the key and value use the Confluent wire format (a `0x00` magic byte plus a
4-byte schema id, then the Avro payload), uniformly for record and primitive schema types. Both sides
depend entirely on Schema Registry: there is no caller-supplied schema override.

- `consumeGenericRecord(topic)` returns values as `Either[PullError, GenericData.Record]`. Each side's
  bytes are decoded against the latest registered schema for its subject. A malformed or
  non-conforming record (including one written under an older, now-incompatible schema) becomes a
  per-record `PullError` rather than terminating the stream.
- `produceGenericRecord(topic)` fetches the write schema from Schema Registry before producing. If the
  `<topic>-key` or `<topic>-value` subject is not registered, it raises `TopicSchemaAbsent`; register
  the schema first.
- `ProduceGenericRecord.jackson` parses Avro's JSON representation against the fetched schema before
  producing.

```scala
val genericConsumer = kafka.consumeGenericRecord("legacy-events")

val decoded =
  genericConsumer
    .updateConfig(_.withGroupId("legacy-reader"))
    .subscribe
    .evalMap(record => IO.println(record.record.value))
    .compile
    .drain
```

The generic connector handles only registry-backed topics. A topic whose values were written with
plain Kafka primitive Serdes (no Confluent prefix, nothing registered) is not readable here; use the
typed `consume(topic)` with the matching deserializers instead. One registry-backed primitive type is
also unsupported: `BYTES` is written by Confluent's serializer without an Avro length prefix and so
does not round-trip through the generic connector.

## Record model

The `record` package provides immutable representations and conversions among Java Kafka, fs2-kafka,
JSON, Avro, and Protobuf forms.

| Type | Purpose |
| --- | --- |
| `NJHeader` | Header key and byte value, with fs2/Java/Protobuf conversions |
| `NJProducerRecord[K, V]` | Optional partition, offset, timestamp, headers, key, and value for producing or persistence |
| `NJConsumerRecord[K, V]` | Consumer coordinates, timestamp type, sizes, headers, leader epoch, and optional key/value |
| `MetaInfo` | Payload-independent topic, partition, offset, timestamp, and serialized-size metadata |
| `ZonedConsumerRecord[K, V]` / `ZonedMetaInfo` | Record metadata with timestamps resolved in a selected zone |

`NJConsumerRecord.toNJProducerRecord` supports republishing while preserving metadata. Builder
methods on `NJProducerRecord` set or clear partition, timestamp, headers, key, and value. Kafka null
keys and tombstone values map to `None` in the `NJ*Record` types and back to `null` when converted to
Kafka records.

`TopicPartitionMap[V]` stores values in deterministic topic/partition order and supports alignment,
flattening, and JSON encoding. `OffsetRange` uses a half-open interval `[from, until)`, with `to`
representing `until - 1`.

## Kafka Streams

### Run a topology

`buildStreams` supplies a Kafka `StreamsBuilder` and a schema-aware `StreamsSerde`:

```scala
import scala.concurrent.duration.*

val streams =
  kafka
    .buildStreams("audit-uppercase") { (builder, serde) =>
      val strings = serde.consumed(Primitive[String], Primitive[String])
      val output = serde.produced(Primitive[String], Primitive[String])

      builder
        .stream[String, String]("audit-input", strings)
        .mapValues((value: String) => value.toUpperCase)
        .to("audit-output", output)
    }
    .withStartupTimeout(30.seconds)
    .withCloseTimeout(30.seconds)

val runStreams: IO[Unit] = streams.runForever.compile.drain
```

`StreamsSerde` also builds `Grouped`, `Repartitioned`, `Joined`, and `StreamJoined` configurations.
The topology is lazy and can be inspected through `streams.topology` without starting Kafka Streams.

At runtime, `kafkaStreams` emits the managed instance after it reaches `RUNNING`, completes on
`NOT_RUNNING`, and fails on `ERROR`. `runForever` keeps the application running without emitting the
instance. The default startup timeout is infinite, the default close timeout is 30 seconds, and the
default transition logger is a no-op. `withTransitionLog` can publish state changes through a chosen
`Log[F]`.

### State stores

`kafka.store(topic)` creates `StateStores[K, V]` using the topic name as the store name and the topic's
registered Serdes. It supports:

- Persistent, timestamped, in-memory, and LRU key-value stores.
- Persistent and in-memory window stores.
- Persistent and in-memory session stores.
- `StoreBuilder` values for explicit topology registration.
- `Materialized` values for tables and aggregations.
- `StoreQueryParameters` for interactive read-only queries.

```scala
val stores = kafka.store(auditTopic)
val materialized = stores.persistentKeyValueStore.materialized
val query = stores.queries.keyValueStore
```

Retention and window-size overloads accept Java or Scala durations where provided. State-store names
are externally visible in Kafka Streams changelog topics and interactive queries, so changing the
`TopicDef` name also changes the store identity.

## Administration

`KafkaContext` exposes three administrative levels:

| Entry point | Scope | Examples |
| --- | --- | --- |
| `kafka.admin` | Raw fs2-kafka `KafkaAdminClient` | Arbitrary broker administration |
| `kafka.admin(topic)` | `AdminTopic` | Create, mirror, inspect partitions/groups/ACLs, query offset ranges, retrieve records, delete |
| `kafka.admin(topic, groupId)` | `AdminTopicGroup` | Lag, commits, offset resets, and offset deletion |
| `kafka.schemaRegistry(topic)` | `TopicSchemaRegistry` | Register, fetch, and delete key/value schema subjects |

Read-only examples:

```scala
val topicDetails =
  kafka.admin("audit-events").use { admin =>
    (admin.partitionsFor, admin.groups, admin.acls).tupled
  }

val consumerLag =
  kafka.admin("audit-events", "audit-indexer").use(_.lagBehind)
```

Administrative mutations include topic creation, topic mirroring, offset commits and resets,
consumer-group offset deletion, and Schema Registry subject deletion. Topic deletion is deliberately
named `iDefinitelyWantToDeleteTheTopicAndUnderstoodItsConsequence`; it permanently removes the topic
and its data.

`kafka.ungroup(groupId, keeps)` attempts to remove that group's offsets from every topic except the
names in `keeps`. It returns only topics whose offset deletion succeeded. Review the returned list
rather than assuming every requested deletion completed.

## Wire format and compatibility

The following values are externally observed or persisted and must remain stable unless a migration
is planned:

- Kafka topic names and consumer group ids.
- Schema Registry subjects `<topic>-key` and `<topic>-value`.
- Confluent envelopes for Avro, JSON Schema, and Protobuf values.
- Kafka header keys and byte values.
- Kafka Streams application ids and state-store names.
- JSON, Avro, and Protobuf record field names.

The built-in record schemas use these stable names:

| Record | Serialized fields |
| --- | --- |
| `NJHeader` | `key`, `value` |
| `NJProducerRecord` | `topic`, `partition`, `offset`, `timestamp`, `headers`, `key`, `value` |
| `NJConsumerRecord` | `topic`, `partition`, `offset`, `timestamp`, `timestampType`, `headers`, `leaderEpoch`, `serializedKeySize`, `serializedValueSize`, `key`, `value` |
| `MetaInfo` | `topic`, `partition`, `offset`, `timestamp`, `timestampType`, `serializedKeySize`, `serializedValueSize` |
| `TopicPartition` JSON | `topic`, `partition` |
| `TopicPartitionMap` JSON entries | `topic`, `partition`, `value` |
| `PullError.toJson` | `key.error` or `value.error` |
| `StateTransition` JSON | `event`, `applicationId`, `oldState`, `newState` |
| `ProtoHeader` | `key`, `value` with field numbers 1 and 2 |
| `ProtoConsumerRecord` | `topic`, `partition`, `offset`, `timestamp`, `timestamp_type`, `headers`, `leader_epoch`, `serialized_key_size`, `serialized_value_size`, `key`, `value` with field numbers 1 through 11 |

The Avro record names are `nanjin.kafka.NJProducerRecord`, `nanjin.kafka.NJConsumerRecord`, and
`nanjin.kafka.header`. A Scala identifier rename is wire-safe only when it does not alter these
serialized names, field keys, topic/subject names, headers, application ids, or store names.

Schema compatibility for structured formats is governed by Schema Registry. The generic Avro
connector derives its schema entirely from the registry: the producer fetches the registered write
schema, and the consumer decodes against the latest registered schema for each subject.

## Resource and operational guidance

- Keep credentials and JAAS strings outside source code, and avoid logging `KafkaSettings` when they
  contain secrets.
- Configure TLS and Schema Registry authentication explicitly for non-local deployments.
- Keep producers, consumers, admin clients, and Kafka Streams instances inside their `Resource` or
  `Stream` scopes so close operations and background fibers are finalized.
- Use long-lived producer resources or sinks for sustained throughput; reserve one-shot producer
  methods for infrequent work.
- Commit consumer offsets only after the corresponding side effects succeed. Choose auto-commit,
  per-record commits, batched commits, or manual partition commits deliberately.
- Use unique consumer group ids for independent readers and integration tests. Reusing a group resumes
  from its committed offsets.
- Use `attemptConsume` or `consumeGenericRecord` when malformed records should remain data rather than
  terminate the stream. Inspect every `Left`; do not silently discard poison records.
- Bound application shutdown and startup with `withCloseTimeout` and `withStartupTimeout` when an
  infinite wait is unacceptable.
- Treat topic deletion, schema deletion, and group-offset deletion as destructive operations.

## Source map and validation

| Area | Source |
| --- | --- |
| Main entry point | [`KafkaContext.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/KafkaContext.scala) |
| Settings | [`KafkaSettings.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/config/KafkaSettings.scala) |
| Topic definitions | [`TopicDef.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/TopicDef.scala) |
| Primitive and structured Serdes | [`Primitive.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/serdes/Primitive.scala), [`Structured.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/serdes/Structured.scala) |
| Type transformations | [`BiTransform.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/serdes/BiTransform.scala) |
| Typed producer and consumer | [`ProduceKafka.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/connector/ProduceKafka.scala), [`ConsumeKafka.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/connector/ConsumeKafka.scala) |
| Generic Avro connectors | [`ProduceGenericRecord.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/connector/ProduceGenericRecord.scala), [`ConsumeGenericRecord.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/connector/ConsumeGenericRecord.scala) |
| Record model | [`NJProducerRecord.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/record/NJProducerRecord.scala), [`NJConsumerRecord.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/record/NJConsumerRecord.scala) |
| Kafka Streams | [`KafkaStreamsBuilder.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/streaming/KafkaStreamsBuilder.scala), [`StateStores.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/streaming/StateStores.scala) |
| Administration | [`AdminTopic.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/admins/AdminTopic.scala), [`AdminTopicGroup.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/admins/AdminTopicGroup.scala) |
| Schema Registry | [`TopicSchemaRegistry.scala`](../src/main/scala/com/github/chenharryhua/nanjin/kafka/admins/TopicSchemaRegistry.scala) |

Offline compilation:

```shell
sbt 'kafka / Test / compile'
```

Representative offline API and model tests:

```shell
sbt "kafka/testOnly mtest.kafka.KafkaContextTest mtest.kafka.KafkaSettingsTest mtest.kafka.TopicDefTest mtest.kafka.KafkaStreamsPureTest mtest.kafka.RecordPureTest"
```

The full test suite includes broker-backed integration tests and expects Kafka at `localhost:9092`
and Schema Registry at `http://localhost:8081`:

```shell
sbt 'kafka / test'
```
