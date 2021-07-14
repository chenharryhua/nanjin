package com.github.chenharryhua.nanjin.http.auth

import java.io.File
import java.nio.file.Files
import java.security.KeyFactory
import java.security.interfaces.RSAPrivateKey
import java.security.spec.PKCS8EncodedKeySpec

private[auth] object encryption {
  //openssl pkcs8 -topk8 -inform PEM -outform DER -nocrypt -in key.pem -out pkcs8.key
  def pkcs8(bytes: Array[Byte]): RSAPrivateKey =
    KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(bytes)).asInstanceOf[RSAPrivateKey]

  def pkcs8(file: File): RSAPrivateKey =
    pkcs8(Files.readAllBytes(file.toPath))

}
