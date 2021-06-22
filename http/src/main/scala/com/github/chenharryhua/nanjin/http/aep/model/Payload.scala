package com.github.chenharryhua.nanjin.http.aep.model

import cats.Show
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax._
import io.circe.{Codec, Json}

final case class Meta(
  endpoint: String,
  schemaVersion: String,
  schemaId: String,
  datasetId: String,
  source: String
)

final case class Payload(meta: Meta, aepBody: Json)

object Payload {
  implicit val payloadShow: Show[Payload]   = _.asJson.noSpaces
  implicit val payLoadCodec: Codec[Payload] = deriveCodec[Payload]
}

final case class AepMessage[A](header: AepHeader, body: AepBody[A])

final case class AepHeader(schemaRef: SchemaRef, imsOrgId: String, datasetId: String)

final case class SchemaRef(id: String, contentType: String)

final case class AepBody[A](xdmMeta: XdmMeta, xdmEntity: A)
final case class XdmMeta(schemaRef: SchemaRef)
