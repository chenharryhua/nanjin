package mtest.spark.persist

import org.scalatest.Sequential

//  sbt "spark/testOnly com.github.chenharryhua.nanjin.spark.persist.PersistTests"

class PersistTests extends Sequential(
      new ConcurrencyTest,
      new AvroTest,
      new BinAvroTest,
      new ParquetTest,
      new CirceTest,
      new JacksonTest,
      new KantanCsvTest,
      new TextTest,
      new ObjectFileTest
    )
