package com.github.chenharryhua.nanjin.http.auth

import better.files.Resource
import org.scalatest.funsuite.AnyFunSuite
import sun.security.util.Pem

import java.io.File
import java.nio.file.Files
class EncryptionTest extends AnyFunSuite {
  test("pkcs8") {
    assert(privateKey.pkcs8File(new File(Resource.getUrl("pkcs8.key").getPath)).getAlgorithm == "RSA")
  }
  test("sign") {
    val src = Resource.getAsString("key.pem").split(System.lineSeparator()).dropRight(1).tail.mkString
    val pem = Pem.decode(src)
    pem.foreach(print)
    println("----")
    Files.readAllBytes(new File(Resource.getUrl("pkcs8.key").getPath).toPath).foreach(print)
  }
}
