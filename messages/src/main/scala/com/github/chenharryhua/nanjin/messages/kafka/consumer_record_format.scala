package com.github.chenharryhua.nanjin.messages.kafka

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.google.protobuf.DescriptorProtos.*

import java.nio.charset.StandardCharsets

private object consumer_record_format {
  def buildJsonNode[K, V](record: NJConsumerRecord[K, V])(k: K => JsonNode, v: V => JsonNode): JsonNode = {
    val root: ObjectNode = globalObjectMapper.createObjectNode()
    root.put("topic", record.topic)
    root.put("partition", record.partition)
    root.put("offset", record.offset)
    root.put("timestamp", record.timestamp)
    root.put("timestampType", record.timestampType)

    record.serializedKeySize match {
      case Some(value) => root.put("serializedKeySize", value)
      case None        => root.putNull("serializedKeySize")
    }
    record.serializedValueSize match {
      case Some(value) => root.put("serializedValueSize", value)
      case None        => root.putNull("serializedValueSize")
    }
    record.leaderEpoch match {
      case Some(value) => root.put("leaderEpoch", value)
      case None        => root.putNull("leaderEpoch")
    }

    val arr: ArrayNode = globalObjectMapper.createArrayNode()
    record.headers.map { hd =>
      val node = globalObjectMapper.createObjectNode()
      // according to the spec, header's key should not be null
      if (hd.value == null) node.putNull(hd.key)
      else
        node.put(hd.key, new String(hd.value, StandardCharsets.UTF_8))

      arr.add(node)
    }
    root.set[ArrayNode]("headers", arr)

    record.key.map(k) match {
      case Some(value) => root.set("key", value)
      case None        => root.putNull("key")
    }

    record.value.map(v) match {
      case Some(value) => root.set("value", value)
      case None        => root.putNull("value")
    }

    root
  }

  private val headerProto: DescriptorProto = DescriptorProto
    .newBuilder()
    .setName("Header")
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("key")
        .setNumber(1)
        .setType(FieldDescriptorProto.Type.TYPE_STRING)
    )
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("value")
        .setNumber(2)
        .setType(FieldDescriptorProto.Type.TYPE_BYTES)
    )
    .build()

  private val recordProto: DescriptorProto = DescriptorProto
    .newBuilder()
    .setName("KafkaConsumerRecord")
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("topic")
        .setNumber(1)
        .setType(FieldDescriptorProto.Type.TYPE_STRING))
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("partition")
        .setNumber(2)
        .setType(FieldDescriptorProto.Type.TYPE_INT32))
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("offset")
        .setNumber(3)
        .setType(FieldDescriptorProto.Type.TYPE_INT64))
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("timestamp")
        .setNumber(4)
        .setType(FieldDescriptorProto.Type.TYPE_INT64))
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("timestampType")
        .setNumber(4)
        .setType(FieldDescriptorProto.Type.TYPE_INT32))
    .addField(FieldDescriptorProto
      .newBuilder()
      .setName("key")
      .setNumber(5)
      .setType(FieldDescriptorProto.Type.TYPE_BYTES))
    .addField(FieldDescriptorProto
      .newBuilder()
      .setName("value")
      .setNumber(6)
      .setType(FieldDescriptorProto.Type.TYPE_BYTES))
    .addField(
      FieldDescriptorProto
        .newBuilder()
        .setName("headers")
        .setNumber(7)
        .setTypeName("Header") // nested type
        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
    .addNestedType(headerProto)
    .build()

  def buildDynamicMessage[K, V](record: NJConsumerRecord[K, V]) = {
    println(record) //Todo
    recordProto
  }

}
