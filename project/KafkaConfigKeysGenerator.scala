import sbt.*

import java.lang.reflect.Field
import java.net.URLClassLoader

object KafkaConfigKeysGenerator {

  def generate(out: File, classpath: Seq[File]): Seq[File] = {
    val classes = Seq(
      "org.apache.kafka.clients.consumer.ConsumerConfig",
      "org.apache.kafka.clients.producer.ProducerConfig",
      "org.apache.kafka.clients.admin.AdminClientConfig",
      "org.apache.kafka.streams.StreamsConfig",
      "io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig",
      "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig",
      "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig",
      "io.confluent.kafka.serializers.KafkaAvroSerializerConfig",
      "io.confluent.kafka.serializers.KafkaAvroDeserializerConfig",
      "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializerConfig",
      "io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig"
    )

    IO.createDirectory(out)

    val loader = new URLClassLoader(classpath.map(_.toURI.toURL).toArray, null)

    val generatedFiles =
      try
        classes.map { className =>
          val cls = Class.forName(className, false, loader)

          val simpleName = cls.getSimpleName + "Keys"

          val classFields = cls.getFields
          val allNames = classFields.map(_.getName.toUpperCase).toSet

          def isConfigKeyField(field: Field): Boolean = {
            val modifiers = field.getModifiers
            val name = field.getName.toUpperCase
            val hasDocOrDefault =
              allNames.contains(s"${name}_DOC") ||
              allNames.contains(s"${name}_DOCS") ||
              allNames.contains(s"${name}_D0C") ||
              allNames.contains(s"${name}_DEFAULT")
            java.lang.reflect.Modifier.isPublic(modifiers) &&
            java.lang.reflect.Modifier.isStatic(modifiers) &&
            java.lang.reflect.Modifier.isFinal(modifiers) &&
            field.getType == classOf[String] &&
            !field.isAnnotationPresent(classOf[java.lang.Deprecated]) &&
            !name.endsWith("_DOC") &&
            !name.endsWith("_DOCS") &&
            !name.endsWith("_D0C") &&
            !name.endsWith("_DEFAULT") &&
            (name.endsWith("_CONFIG") || hasDocOrDefault)
          }

          val fields =
            classFields
              .filter(isConfigKeyField)
              .sortBy(_.getName)

          val methods =
            fields
              .map { f =>
                val ownerClassName = f.getDeclaringClass.getName
                s"""  inline final def ${f.getName}: String =
                   |    $ownerClassName.${f.getName}
                   |""".stripMargin
              }
              .mkString("\n")

          val content =
            s"""package com.github.chenharryhua.nanjin.kafka.config
               |
               |sealed trait $simpleName {
               |$methods
               |}
               |
               |object $simpleName extends $simpleName
               |""".stripMargin

          val file = out / s"$simpleName.scala"
          IO.write(file, content)
          file
        }
      finally loader.close()

    val generatedNames = generatedFiles.map(_.name).toSet
    IO.listFiles(out).filter(f => f.ext == "scala" && !generatedNames.contains(f.name)).foreach(IO.delete)

    generatedFiles
  }
}
