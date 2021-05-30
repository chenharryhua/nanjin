package com.github.chenharryhua.nanjin.salesforce.client

import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._
import org.http4s._

final case class SalesforceResponse(
  httpVersion: HttpVersion,
  method: Method,
  uri: Uri,
  reqeustHeaders: Headers,
  requestBody: String,
  status: Status,
  responseHeaders: Headers,
  responseBody: String)

object SalesforceResponse {
  implicit private val encoderUri: Encoder[Uri]        = Encoder[String].contramap(_.renderString)
  implicit private val encoerMethod: Encoder[Method]   = Encoder[String].contramap(_.renderString)
  implicit private val encoerHeaders: Encoder[Headers] = Encoder[String].contramap(_ => "headers")

  implicit val encoderSalesforceResponse: Encoder[SalesforceResponse] =
    io.circe.generic.semiauto.deriveEncoder[SalesforceResponse]
}

final case class SalesforceException(response: SalesforceResponse, description: String)
    extends Exception(response.asJson.noSpaces)

final case class HttpPath(private val str: String) {

  private def mkString(inStr: String): String =
    "/" + inStr.split("/").toList.filter(_.nonEmpty).mkString("/")

  val value: String                     = mkString(str)
  def append(other: String): HttpPath   = HttpPath(mkString(s"$value/$other"))
  def append(other: HttpPath): HttpPath = append(other.value)
}
