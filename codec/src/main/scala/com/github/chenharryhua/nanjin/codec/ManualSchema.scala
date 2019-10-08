package com.github.chenharryhua.nanjin.codec

import org.apache.avro.Schema
import cats.tagless.finalAlg
import com.sksamuel.avro4s.SchemaFor

@finalAlg
trait ManualSchema[A] {
  def schema: Schema
  final def validate(implicit ev: SchemaFor[A]): Boolean = false
}
