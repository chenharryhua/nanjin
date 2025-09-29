package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.effect.kernel.{Resource, Sync}
import cats.implicits.{catsSyntaxApplicativeError, catsSyntaxEq, toFunctorOps}
import cats.kernel.Eq
import io.circe.{Decoder, Encoder}

import java.net.{HttpURLConnection, InetAddress, URL}
import scala.io.{BufferedSource, Source}
import scala.util.Try

final class HostName private (val value: String) extends AnyVal {

  override def toString: String = value
}

object HostName {

  implicit final val showHostName: Show[HostName] = Show.fromToString
  implicit final val eqHostName: Eq[HostName] = Eq.instance((a, b) => a.value === b.value)

  implicit final val encoderHostName: Encoder[HostName] = Encoder.encodeString.contramap(_.value)
  implicit final val decoderHostName: Decoder[HostName] = Decoder.decodeString.map(new HostName(_))

  def apply[F[_]](implicit F: Sync[F]): F[HostName] = {

    def http_get(url: URL): Resource[F, Option[HttpURLConnection]] =
      Resource.make(F.pure(url.openConnection() match {
        case conn: HttpURLConnection =>
          conn.setRequestMethod("GET")
          conn.setConnectTimeout(1000)
          conn.setReadTimeout(1000)
          Some(conn)
        case _ => None
      }))(conn => F.blocking(conn.map(_.disconnect())).void)

    def combine(a: Option[String], b: Option[String]): Option[String] = (a, b) match {
      case (None, None)        => None
      case (x @ Some(_), None) => x
      case (None, y @ Some(_)) => y
      case (Some(x), Some(y))  => Some(s"$x/$y")
    }

    val aws_ec2_ipv4: F[Option[String]] =
      http_get(new URL("http://169.254.169.254/latest/meta-data/local-ipv4"))
        .use(conn =>
          F.blocking(conn.map { c =>
            val bs: BufferedSource = Source.fromInputStream(c.getInputStream)
            try bs.getLines().mkString.ensuring(_.trim.nonEmpty)
            finally bs.close()
          }))
        .attempt
        .map(_.toOption.flatten)

    val local_host: Option[String] =
      Try(Option(InetAddress.getLocalHost.getHostName.ensuring(_.trim.nonEmpty))).toOption.flatten

    aws_ec2_ipv4.map(combine(_, local_host).fold(new HostName("unknown"))(new HostName(_)))
  }
}
