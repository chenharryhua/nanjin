package example.database

import org.scalatest.DoNotDiscover
import org.scalatest.funsuite.AnyFunSuite

@DoNotDiscover
class ExampleDatabase extends AnyFunSuite {
  test("1.persist db table to disk") {}
  test("2.load db table on disk to db") {}
  test("3.load db table on disk to kafka") {}
  test("4.dump kafka to db table") {}
}
