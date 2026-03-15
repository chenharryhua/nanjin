package example

import example.database.ExampleDatabase
import org.scalatest.Sequential

class RunAll extends Sequential(
      new ExampleDatabase,
    )
