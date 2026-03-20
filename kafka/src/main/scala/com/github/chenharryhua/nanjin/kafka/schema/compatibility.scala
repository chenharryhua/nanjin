package com.github.chenharryhua.nanjin.kafka.schema

import org.apache.avro.{Schema, SchemaCompatibility}
import scala.jdk.CollectionConverters.ListHasAsScala

def backwardCompatibility(a: Schema, b: Schema): List[SchemaCompatibility.Incompatibility] =
  SchemaCompatibility.checkReaderWriterCompatibility(a, b).getResult.getIncompatibilities.asScala.toList

def forwardCompatibility(a: Schema, b: Schema): List[SchemaCompatibility.Incompatibility] =
  backwardCompatibility(b, a)
