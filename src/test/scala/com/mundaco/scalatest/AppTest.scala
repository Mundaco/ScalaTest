package com.mundaco.scalatest

import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite


class AppTest extends FunSuite {

  App.init()

  test("App.init") {

    assert(App.spark != null)

  }

  test("App.readCSV") {

    /*
    val df = App.readCSV("test")
    assert(df != null)
    val df1 = df.select("id")
    assert(df1.isInstanceOf[DataFrame])
    assert(df1 != null)
  */
  }

  App.close()
}
