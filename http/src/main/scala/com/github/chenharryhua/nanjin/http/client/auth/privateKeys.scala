package com.github.chenharryhua.nanjin.http.client.auth

import cats.ApplicativeError
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.openssl.{PEMKeyPair, PEMParser}
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter

import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.{KeyFactory, PrivateKey}
import java.security.spec.PKCS8EncodedKeySpec
import scala.util.Try

object privateKeys {
  // openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt -in key.pem -out pkcs8.key
  def pkcs8(bytes: Array[Byte]): Try[PrivateKey] = Try(
    KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(bytes)))

  def pkcs8(path: Path): Try[PrivateKey] =
    Try(Files.readAllBytes(path)).flatMap(pkcs8)

  def pkcs8F[F[_]](bytes: Array[Byte])(implicit F: ApplicativeError[F, Throwable]): F[PrivateKey] =
    F.fromTry(pkcs8(bytes))

  def pkcs8F[F[_]](path: Path)(implicit F: ApplicativeError[F, Throwable]): F[PrivateKey] =
    F.fromTry(pkcs8(path))

  def pem(content: String): Try[PrivateKey] = Try {
    val converter: JcaPEMKeyConverter = new JcaPEMKeyConverter
    new PEMParser(new StringReader(content)).readObject() match {
      case pem: PEMKeyPair      => converter.getKeyPair(pem).getPrivate
      case info: PrivateKeyInfo => converter.getPrivateKey(info)
      case unknown => sys.error(s"unknown: ${unknown.toString}, should be PEMKeyPair or PrivateKeyInfo")
    }
  }

  def pem(path: Path): Try[PrivateKey] =
    Try(Files.readAllBytes(path)).flatMap(bs => pem(new String(bs, StandardCharsets.UTF_8)))

  def pemF[F[_]](content: String)(implicit F: ApplicativeError[F, Throwable]): F[PrivateKey] =
    F.fromTry(pem(content))

  def pemF[F[_]](path: Path)(implicit F: ApplicativeError[F, Throwable]): F[PrivateKey] =
    F.fromTry(pem(path))
}
