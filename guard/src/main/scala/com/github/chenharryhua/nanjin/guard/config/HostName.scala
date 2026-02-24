package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.effect.kernel.{Resource, Sync}
import cats.kernel.Eq
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.functor.toFunctorOps
import io.circe.generic.JsonCodec

import java.net.{HttpURLConnection, InetAddress, URI, URL}
import scala.io.{BufferedSource, Source}
import scala.util.Try

@JsonCodec
final case class HostName private (ec2: Option[String], local: Option[String]) {

  val value: String = (ec2, local) match {
    case (None, None)       => "unknown"
    case (Some(x), None)    => x
    case (None, Some(y))    => y
    case (Some(x), Some(y)) => s"$x/$y"
  }

  override val toString: String = value
}

object HostName {

  implicit final val showHostName: Show[HostName] = Show.fromToString
  implicit final val eqHostName: Eq[HostName] = Eq.instance((a, b) => a.value === b.value)

  def apply[F[_]](implicit F: Sync[F]): F[HostName] = {

    def http_get(url: URL): Resource[F, Option[HttpURLConnection]] =
      Resource.make(F.blocking(url.openConnection() match {
        case conn: HttpURLConnection =>
          conn.setRequestMethod("GET")
          conn.setConnectTimeout(1000)
          conn.setReadTimeout(1000)
          Some(conn)
        case _ => None
      }))(conn => F.blocking(conn.foreach(_.disconnect())).void)

    val aws_ec2_ipv4: F[Option[String]] =
      http_get(URI.create("http://169.254.169.254/latest/meta-data/local-ipv4").toURL)
        .use(conn =>
          F.blocking(conn.map { c =>
            val bs: BufferedSource = Source.fromInputStream(c.getInputStream)
            try Option(bs.getLines().mkString.trim).filter(_.nonEmpty)
            finally bs.close()
          }))
        .attempt
        .map(_.toOption.flatMap(_.flatten))

    val local_host: Option[String] =
      Try(Option(InetAddress.getLocalHost.getHostName).filter(_.trim.nonEmpty)).toOption.flatten

    aws_ec2_ipv4.map(aws => new HostName(aws, local_host))
  }
}
