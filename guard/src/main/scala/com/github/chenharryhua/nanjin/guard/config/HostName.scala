package com.github.chenharryhua.nanjin.guard.config

import cats.Show
import cats.effect.kernel.{Resource, Sync}
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import io.circe.Codec

import java.net.{HttpURLConnection, InetAddress, URI, URL}
import java.nio.charset.StandardCharsets
import scala.io.{BufferedSource, Source}
import scala.util.Try

final case class HostName private (ec2: Option[String], local: Option[String]) derives Codec.AsObject {

  val value: String = (ec2, local) match {
    case (None, None)       => "unknown"
    case (Some(x), None)    => x
    case (None, Some(y))    => y
    case (Some(x), Some(y)) => s"$x/$y"
  }

  override val toString: String = value
}

object HostName {

  given Show[HostName] = Show.fromToString

  // EC2 instance metadata endpoints. The link-local address is unroutable anywhere else, so off
  // an instance every probe below ends in a connect timeout.
  private val TOKEN_URL: String = "http://169.254.169.254/latest/api/token"
  private val IPV4_URL: String = "http://169.254.169.254/latest/meta-data/local-ipv4"
  private val TOKEN_TTL_HEADER: String = "X-aws-ec2-metadata-token-ttl-seconds"
  private val TOKEN_TTL_SECONDS: String = "21600"
  private val TOKEN_HEADER: String = "X-aws-ec2-metadata-token"

  /** Outcome of asking the metadata service for an IMDSv2 session token. */
  private enum Imds {
    case Absent // nothing answered the address: not an EC2 instance, or IMDS is turned off
    case Tokenless // something answered but minted no token: IMDSv1 only
    case Token(value: String)
  }

  def apply[F[_]](using F: Sync[F]): F[HostName] = {

    // F.blocking rather than F.interruptible on purpose: Thread.interrupt does not unblock a
    // HttpURLConnection socket read, so cancellation would wait out the timeout either way. The
    // connect and read timeouts are what bound a service start.
    def http_conn(
      url: URL,
      method: String,
      headers: Map[String, String]): Resource[F, Option[HttpURLConnection]] =
      Resource.make(F.blocking(url.openConnection() match {
        case conn: HttpURLConnection =>
          conn.setRequestMethod(method)
          conn.setConnectTimeout(1000)
          conn.setReadTimeout(1000)
          headers.foreach { case (key, value) => conn.setRequestProperty(key, value) }
          Some(conn)
        case _ => None
      }))(conn => F.blocking(conn.foreach(_.disconnect())).void)

    def read_body(conn: HttpURLConnection): Option[String] = {
      val bs: BufferedSource =
        Source.fromInputStream(conn.getInputStream, StandardCharsets.UTF_8.name())
      try Option(bs.getLines().mkString.trim).filter(_.nonEmpty)
      finally bs.close()
    }

    val imds_token: F[Imds] =
      http_conn(URI.create(TOKEN_URL).toURL, "PUT", Map(TOKEN_TTL_HEADER -> TOKEN_TTL_SECONDS))
        .use {
          case None       => F.pure(Imds.Absent)
          case Some(conn) =>
            // getResponseCode sends the request. It throws when nothing answers the address, and
            // returns a status when IMDS is present but declines to mint a token.
            F.blocking(conn.getResponseCode match {
              case HttpURLConnection.HTTP_OK => read_body(conn).fold[Imds](Imds.Tokenless)(Imds.Token(_))
              case _                         => Imds.Tokenless
            })
        }
        .attempt
        .map(_.getOrElse(Imds.Absent))

    def ec2_ipv4(headers: Map[String, String]): F[Option[String]] =
      http_conn(URI.create(IPV4_URL).toURL, "GET", headers)
        .use(conn => F.blocking(conn.flatMap(read_body)))
        .attempt
        .map(_.toOption.flatten)

    /*
     * IMDSv2 first: where a token is required, an unauthenticated GET returns 401 and the ec2
     * address would silently go missing. A token is asked for first for a second reason - its
     * failure mode tells us whether to probe at all. Nothing answering (Absent) means no metadata
     * service, so the ipv4 GET is skipped and a non-EC2 host pays one connect timeout instead of
     * two. A read timeout is also treated as Absent: an IMDS that cannot answer within the timeout
     * has nothing to tell us.
     */
    val aws_ec2_ipv4: F[Option[String]] =
      imds_token.flatMap {
        case Imds.Absent       => F.pure(None)
        case Imds.Tokenless    => ec2_ipv4(Map.empty)
        case Imds.Token(value) => ec2_ipv4(Map(TOKEN_HEADER -> value))
      }

    val local_host: F[Option[String]] =
      F.blocking {
        Try(Option(InetAddress.getLocalHost.getHostName).filter(_.trim.nonEmpty)).toOption.flatten
      }

    for {
      ec2 <- aws_ec2_ipv4
      local <- local_host
    } yield new HostName(ec2, local)
  }
}
