package com.mundaco.scalatest

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite


class AppTest extends FunSuite {

  App.init()

  test("App.init") {

    assert(App.spark != null)

  }

  test("App.main") {

    App.main(null)
  }

  App.close()
}
