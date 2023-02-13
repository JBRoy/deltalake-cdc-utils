package com.zeotap.deltalake.cdc

import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CDCDemo {

  val rows = Seq(
    Row(1, "Male", 25),
    Row(2, "Male", 25),
    Row(3, "Female", 35))

  val updates = List(
    Row(1, "Male", 35),
    Row(2, "Male", 100),
    Row(2, "Male", 101),
    Row(2, "Male", 102),
    Row(4, "Female", 18),
    Row(1, "Male", 55),
    Row(2, "Male", 65),
    Row(2, "Male", 66),
    Row(2, "Male", 67),
    Row(4, "Female", 25),
    Row(4, "Male", 45)
  )
  val schema: StructType = StructType(Array(
    StructField("id", IntegerType, false),
    StructField("gender", StringType, false),
    StructField("age", IntegerType, false)
  ))

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    implicit val spark = SparkSession.builder()
      .master("local")
      .appName("DELTA LAKE CDC DEMO")
      .config(conf)
      .getOrCreate()

    val df = createSingleRowDF(spark, rows)
    // Set this to your project path
    val path = "/Users/joydeep/IdeaProjects/deltalake-cdc-utils/src/test/resources/cdcTestForVersions"
    df.write.format("delta").save(path)

    // Reade the Delta Table
    val table = DeltaTable.forPath(path)
    // Apply merge on the table one by one
    updates.foreach(row => {
      val dataFrame = createSingleRowDF(spark, Seq(row))
      table.as("target")
        .merge(dataFrame.as("source"), "target.id = source.id")
        .whenMatched
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    })

    println("debugger stops here")
  }

  def createSingleRowDF(spark: SparkSession, rows: Seq[Row]) =
    spark.createDataFrame(spark.sparkContext.makeRDD(rows), schema)

}
