package com.github.chenharryhua.nanjin.http.auth

import better.files.Resource
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
class EncryptionTest extends AnyFunSuite {
  test("pkcs8") {
    assert(privateKey.pkcs8(new File(Resource.getUrl("pkcs8.key").getPath)).getAlgorithm == "RSA")
  }
}
