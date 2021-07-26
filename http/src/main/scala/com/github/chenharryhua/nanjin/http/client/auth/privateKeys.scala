package com.github.chenharryhua.nanjin.http.client.auth

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.bouncycastle.openssl.{PEMKeyPair, PEMParser}

import java.io.StringReader
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey}
import scala.util.Try

object privateKeys {
  //openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt -in key.pem -out pkcs8.key
  def pkcs8(bytes: Array[Byte]): Try[PrivateKey] = Try(
    KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(bytes)))

  def pkcs8(path: Path): Try[PrivateKey] =
    Try(Files.readAllBytes(path)).flatMap(pkcs8)

  def pem(content: String): Try[PrivateKey] = Try {
    val converter: JcaPEMKeyConverter = new JcaPEMKeyConverter
    new PEMParser(new StringReader(content)).readObject() match {
      case pem: PEMKeyPair      => converter.getKeyPair(pem).getPrivate
      case info: PrivateKeyInfo => converter.getPrivateKey(info)
    }
  }

  def pem(path: Path): Try[PrivateKey] =
    Try(Files.readAllBytes(path)).flatMap(bs => pem(new String(bs, StandardCharsets.UTF_8)))
}
