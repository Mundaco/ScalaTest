package com.mundaco.scalatest


import java.sql.Date

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

object App {

  var spark: SparkSession = _

  final val clients_table_name: String = "Clients"
  case class Client(id: BigInt, name: String)
  val clientSchema: StructType = ScalaReflection.schemaFor[Client].dataType.asInstanceOf[StructType]

  final val orders_table_name: String = "Orders"
  case class Order(id: BigInt, client_id: BigInt, date: Date)
  val orderSchema: StructType = ScalaReflection.schemaFor[Order].dataType.asInstanceOf[StructType]


  def createDatabase(): Unit = {
    val clients = readCSV(clients_table_name, clientSchema)
    clients.createOrReplaceTempView(clients_table_name)
    clients.show()
    spark.sql(s"Drop Table If Exists h_$clients_table_name")
    spark.sql(s"Create Table h_$clients_table_name as select * from $clients_table_name")

    val orders = readCSV(orders_table_name, orderSchema)
    orders.createOrReplaceTempView(orders_table_name)
    orders.show()
    spark.sql(s"Drop Table If Exists h_$orders_table_name")
    spark.sql(s"Create Table h_$orders_table_name as select * from $orders_table_name")
  }

  def main(args: Array[String]): Unit = {

    init()

    //createDatabase()

    val clients = readCSV(clients_table_name, clientSchema)
    val orders = readCSV(orders_table_name, orderSchema)

    orders.filter(orders("date").between("2019-11-18","2019-11-19"))
      .join(clients,clients("id") === orders("client_id"),"left_outer")
      .select("Orders.id","Clients.name", "Orders.date")
      .na.fill("<unknown>", Seq("name"))
      .orderBy("date")
      .show()


    spark.sql(
      "Select " +
        "O.id, If(C.name is null,'<unknown>',C.name) As name, O.date " +
        "from h_Orders O " +
        "left outer join h_Clients C On C.id = O.client_id " +
        "Where O.date Between '2019-11-18' And '2019-11-19' " +
        "Order By O.date"
    ).show()

    System.in.read()

    close()
  }

  def init(): Unit = {
    spark = SparkSession.builder()
      .appName("ScalaTest")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
  }

  def close(): Unit = {
    spark.close()
  }

  def readCSV(name: String, schema: StructType): DataFrame = {

    spark.read
      .schema(schema)
      .csv(s"res/$name.csv").as(name)
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
