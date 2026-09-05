package com.github.chenharryhua.nanjin.kafka.serdes

import cats.Endo
import com.fasterxml.jackson.databind.{JavaType, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.JavaTypeable
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.google.protobuf.DynamicMessage
import com.kjetland.jackson.jsonSchema.{JsonSchemaConfig, JsonSchemaGenerator}
import com.sksamuel.avro4s.{Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import io.circe.{Decoder as JsonDecoder, Encoder as JsonEncoder, Json}
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.json.{JsonSchema, JsonSchemaUtils}
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema
import io.scalaland.chimney.Iso as ChimneyIso
import monocle.Iso as MonocleIso
import org.apache.avro.generic.GenericRecord
import scalapb.{GeneratedMessage, GeneratedMessageCompanion}

import scala.reflect.{classTag, ClassTag}

/** A pair of conversion functions between a Kafka-side representation `A` and an application value `B`.
  *
  * `BiTransform` carries no round-trip law. Some instances are true isomorphisms (the primitive
  * boxed-Java/`Option` pairs, and the `Iso`-derived ones), but the codec-backed instances are not: decoding
  * then re-encoding (`from(to(a))`) need not reproduce the original `A`, since encoding can reorder fields,
  * drop unknowns, fill defaults, or add a schema envelope. Treat `to`/`from` as a best-effort bridge in each
  * direction, not a guaranteed inverse pair.
  *
  * The companion supplies instances for the common bridges used when reading/writing Kafka records: circe
  * `Json`, Avro `GenericRecord`, Protobuf `DynamicMessage`, and Jackson `JsonNode` (all via `KafkaCodec`);
  * boxed Java primitives to `Option` of the Scala primitive (null becomes `None`); and generic derivations
  * from a Chimney or Monocle `Iso`, lifted through `Option`.
  *
  * @tparam A
  *   the Kafka-side representation.
  * @tparam B
  *   the application value type.
  */
sealed trait BiTransform[A, B]:

  /** Convert a Kafka-side value into the application value. */
  def to(a: A): B

  /** Convert an application value back into the Kafka-side representation. */
  def from(b: B): A
end BiTransform

object BiTransform:

  /** Bridge circe `Json` and any `B` with circe codecs. `to` throws the decode error if the JSON does not
    * conform to `B`.
    */
  given [B: {JsonDecoder, JsonEncoder}]: BiTransform[Json, B] =
    new BiTransform[Json, B]:
      private val enc: JsonEncoder[B] = JsonEncoder[B]
      private val dec: JsonDecoder[B] = JsonDecoder[B]

      override def to(a: Json): B =
        dec.decodeJson(a) match
          case Left(ex)     => throw ex // scalafix:ok
          case Right(value) => value

      override def from(b: B): Json = enc(b)
  end given

  /** Bridge Avro `GenericRecord` and `B` via the derived `KafkaCodec.avro` codec. */
  given [B: {SchemaFor, Decoder, Encoder}]: BiTransform[GenericRecord, B] =
    KafkaCodec.avro[B]

  /** Bridge Protobuf `DynamicMessage` and a generated message `B` via `KafkaCodec.protobuf`. */
  given [B <: GeneratedMessage: GeneratedMessageCompanion]: BiTransform[DynamicMessage, B] =
    KafkaCodec.protobuf[B]

  /** Bridge Jackson `JsonNode` and `B` via `KafkaCodec.json`, using the given `ObjectMapper`. */
  given [B: ClassTag](using mapper: ObjectMapper): BiTransform[JsonNode, B] =
    KafkaCodec.json(mapper)

  /*
   * Primitive: boxed Java primitive <-> Option of the Scala primitive. `null` maps to `None`, `Some` maps to
   * the boxed value.
   */
  given BiTransform[java.lang.Integer, Option[Int]] with
    override def from(b: Option[Int]): Integer = b.map(Int.box).orNull
    override def to(a: java.lang.Integer): Option[Int] = Option(a)
  end given

  given BiTransform[java.lang.Long, Option[Long]] with
    override def from(b: Option[Long]): java.lang.Long = b.map(Long.box).orNull
    override def to(a: java.lang.Long): Option[Long] = Option(a)
  end given

  given BiTransform[java.lang.Float, Option[Float]] with
    override def from(b: Option[Float]): java.lang.Float = b.map(Float.box).orNull
    override def to(a: java.lang.Float): Option[Float] = Option(a)
  end given

  given BiTransform[java.lang.Short, Option[Short]] with
    override def from(b: Option[Short]): java.lang.Short = b.map(Short.box).orNull
    override def to(a: java.lang.Short): Option[Short] = Option(a)
  end given

  given BiTransform[java.lang.Double, Option[Double]] with
    override def from(b: Option[Double]): java.lang.Double = b.map(Double.box).orNull
    override def to(a: java.lang.Double): Option[Double] = Option(a)
  end given

  given BiTransform[java.lang.Boolean, Option[Boolean]] with
    override def from(b: Option[Boolean]): java.lang.Boolean = b.map(Boolean.box).orNull
    override def to(a: java.lang.Boolean): Option[Boolean] = Option(a)
  end given

  /*
   * Generic derivations from an existing isomorphism, and lifting through Option.
   */

  /** Derive a `BiTransform` from a Chimney `Iso[A, B]`. */
  given [A, B](using iso: ChimneyIso[A, B]): BiTransform[A, B] =
    new BiTransform[A, B]:
      override def to(a: A): B = iso.first.transform(a)
      override def from(b: B): A = iso.second.transform(b)
  end given

  /** Derive a `BiTransform` from a Monocle `Iso[A, B]`. */
  given [A, B](using iso: MonocleIso[A, B]): BiTransform[A, B] =
    new BiTransform[A, B]:
      override def to(a: A): B = iso.get(a)
      override def from(b: B): A = iso.reverseGet(b)
  end given

  /** Lift a `BiTransform[A, B]` over `Option`, mapping element-wise (`None` stays `None`). */
  given [A, B](using ab: BiTransform[A, B]): BiTransform[Option[A], Option[B]] =
    new BiTransform[Option[A], Option[B]]:
      override def to(a: Option[A]): Option[B] = a.map(ab.to)
      override def from(b: Option[B]): Option[A] = b.map(ab.from)
  end given

end BiTransform

/** A bidirectional codec coupled with its Confluent Schema Registry schema.
  *
  * Use the factory methods on `KafkaCodec` to create codecs. The concrete codec implementations are kept
  * inside the companion so their required schema and transformation typeclass instances are captured at
  * construction time.
  *
  * @tparam S
  *   the concrete Confluent schema type
  * @tparam A
  *   the Kafka-side representation, such as `GenericRecord`, `DynamicMessage`, or `JsonNode`
  * @tparam B
  *   the application value type
  */
sealed trait KafkaCodec[S <: ParsedSchema, A, B] extends BiTransform[A, B]:
  def schema: S
end KafkaCodec

object KafkaCodec:

  /** Creates an Avro codec with its generated Avro schema.
    *
    * The schema is derived from `SchemaFor[B]`; customize Avro schemas through Avro4s annotations or an
    * explicit `SchemaFor[B]` instance.
    */
  def avro[B: {SchemaFor, Decoder, Encoder}]: KafkaAvroCodec[B] =
    new KafkaAvroCodec[B](SchemaFor[B], Decoder[B], Encoder[B])

  /** Creates a Protobuf codec with the schema from the generated message descriptor.
    *
    * The schema is defined by the generated descriptor and therefore by the source `.proto` definition.
    */
  def protobuf[B <: GeneratedMessage: GeneratedMessageCompanion]: KafkaProtobufCodec[B] =
    new KafkaProtobufCodec[B](summon[GeneratedMessageCompanion[B]])

  /** Creates a JSON Schema codec using the supplied Jackson object mapper.
    *
    * JSON Schema generation can be customized immutably with `updateConfig`. This configures schema
    * generation only; Confluent serializer settings are configured separately through `KafkaSettings`.
    */
  def json[B: ClassTag](objectMapper: ObjectMapper): KafkaJsonCodec[B] =
    new KafkaJsonCodec[B](objectMapper, identity)

  /** A codec between an Avro `GenericRecord` and an application value. */
  final class KafkaAvroCodec[B] private[KafkaCodec] (
    schemaFor: SchemaFor[B],
    decoder: Decoder[B],
    encoder: Encoder[B])
      extends KafkaCodec[AvroSchema, GenericRecord, B]:

    override val schema: AvroSchema = AvroSchema(schemaFor.schema)
    private val dec: FromRecord[B] = FromRecord[B](schemaFor.schema)(using decoder)
    private val enc: ToRecord[B] = ToRecord[B](schemaFor.schema)(using encoder)

    override def to(a: GenericRecord): B = dec.from(a)

    override def from(b: B): GenericRecord = enc.to(b)
  end KafkaAvroCodec

  /** A codec between a Protobuf `DynamicMessage` and an application value. */
  final class KafkaProtobufCodec[B <: GeneratedMessage] private[KafkaCodec] (
    gmc: GeneratedMessageCompanion[B])
      extends KafkaCodec[ProtobufSchema, DynamicMessage, B]:

    override val schema: ProtobufSchema = ProtobufSchema(gmc.javaDescriptor)

    override def to(a: DynamicMessage): B = gmc.parseFrom(a.toByteArray)

    override def from(b: B): DynamicMessage =
      DynamicMessage.parseFrom(b.companion.javaDescriptor, b.toByteArray)
  end KafkaProtobufCodec

  /** A codec between a JSON Schema `JsonNode` and an application value. */
  final class KafkaJsonCodec[B: ClassTag] private[KafkaCodec] (
    mapper: ObjectMapper,
    f: Endo[JsonSchemaConfig.JsonSchemaConfigBuilder])
      extends UpdateConfig[JsonSchemaConfig.JsonSchemaConfigBuilder, KafkaJsonCodec[B]]
      with KafkaCodec[JsonSchema, JsonNode, B]:

    /** Generated schema for the runtime class of `B`. */
    override lazy val schema: JsonSchema =
      new JsonSchema(
        new JsonSchemaGenerator(mapper, f(JsonSchemaConfig.builder()).build())
          .generateJsonSchema(classTag[B].runtimeClass))

    override def updateConfig(g: Endo[JsonSchemaConfig.JsonSchemaConfigBuilder]): KafkaJsonCodec[B] =
      new KafkaJsonCodec[B](mapper, g.compose(f))

    private val jt: JavaType = summon[JavaTypeable[B]].asJavaType(mapper.getTypeFactory)

    override def to(a: JsonNode): B = mapper.treeToValue[B](a, jt)

    // Confluent JSON Schema requires the schema envelope.
    override def from(b: B): JsonNode =
      JsonSchemaUtils.envelope(schema, mapper.valueToTree[JsonNode](b))
  end KafkaJsonCodec

end KafkaCodec
