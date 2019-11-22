package com.mundaco.scalatest

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

object App {

  var spark: SparkSession = _


  final val dbname: String = "test"
  case class MyEntry(id: BigInt, name: String)
  val schema: StructType = ScalaReflection.schemaFor[MyEntry].dataType.asInstanceOf[StructType]




  def main(args: Array[String]) = {


    spark = SparkSession.builder()
      .appName("p3")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()


    val df = readCSV(dbname)
      .union(spark.createDataFrame(Seq((5, "Yay!"))))

    println("DataFrame")
    df.show()

    df.createOrReplaceTempView(dbname)
    spark.sql(s"drop table hive_$dbname")
    spark.sql(s"Create Table hive_$dbname as select * from $dbname")
    spark.sql(s"select * from hive_$dbname").show()

    //writeParquet(df)
    //readParquet().select("name").where("id=1").show()

    spark.close()
  }

  def readCSV(name: String): DataFrame = {

    spark.read
      .schema(schema)
      .csv(s"res/$name.csv")


  }

  def writeParquet(df: DataFrame, name: String):Unit = {
    try {
      df.write.parquet(s"res/$name.parquet")
    } catch {
      case e:AnalysisException =>
    }
  }

  def readParquet(name: String): DataFrame = {

    spark.read
      .parquet(s"res/$name.parquet")
  }

}
