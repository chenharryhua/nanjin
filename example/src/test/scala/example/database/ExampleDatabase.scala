package example.database

import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ExampleDatabase extends AnyFunSuite {
  test("persist db table to disk") {}
  test("load db table on disk to db") {}
  test("load db table on disk to kafka") {}
  test("dump kafka to db table") {}
}
