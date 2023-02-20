package com.zeotap.deltalake.cdc

import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions.{desc, row_number, udf}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions}

import scala.collection.mutable

object CDCOps {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .master("local")
      .appName("DELTA LAKE CDC READ BY TIMESTAMP")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    //spark.sparkContext.setLogLevel("OFF")
    // Set this to your project path or any _change_data path
    val cdPath = "/Users/joydeep/IdeaProjects/deltalake-cdc-utils/src/test/resources/cdcTestForVersions/_change_data"
    val table = DeltaTable.forPath(cdPath)
    //table.optimize().executeCompaction()

    val df = getTimestampByPartitions(cdPath)

    val windowSpec = Window.partitionBy("id")
      .orderBy(desc("timestamp"))

    // Fetch using window operation
    val changeDF = df.filter("_change_type in ('update_postimage', 'insert', 'delete') ")
      .withColumn("row_number", row_number.over(windowSpec))
      .filter("row_number = 1")

    // Fetch based on groupByKey
    val gbChangeDF = df.filter("_change_type in ('update_postimage', 'insert', 'delete') ")
      .groupByKey { row => row.getAs[Integer]("id") }(Encoders.INT)
      .reduceGroups { (r1, r2) => {
        if (r1.getAs[Long]("timestamp") > r2.getAs[Long]("timestamp"))
          r1
        else r2
      }
      }

    println("debugger stops here")
  }

  def getTimestampByPartitions(path: String)(implicit spark: SparkSession) = {
    val frame = spark.read.parquet(path).withColumn("file_name", functions.input_file_name())
    val schema = frame.schema
    val finalSchema = schema.add("timestamp", LongType)
    frame.mapPartitions(rows => fetchTimeStampPerPartition(rows))(RowEncoder(finalSchema)) //.show(false)
  }

  def fetchTimeStampPerPartition(rows: Iterator[Row]): Iterator[Row] = {
    val tsMap: scala.collection.mutable.Map[String, Long] = scala.collection.mutable.Map()
    for {
      row <- rows
    } yield appendTSWithRow(row, tsMap)
  }

  def appendTSWithRow(row: Row, tsMap: mutable.Map[String, Long]) = {
    val fileName = row.getAs[String]("file_name")
    if (tsMap.contains(fileName)) {
      Row.fromSeq(row.toSeq ++ Seq(tsMap(fileName)))
    }
    else {
      tsMap += (fileName -> getTimestamp(fileName))
      Row.fromSeq(row.toSeq ++ Seq(tsMap(fileName)))
    }
  }

  def getTimestamp(fileName: String) = {
    val path = new Path(fileName)
    path.getFileSystem(new Configuration).getFileStatus(path).getModificationTime
  }

  def getSizeInBytes(fileName: String) = {
    val path = new Path(fileName)
    path.getFileSystem(new Configuration).getFileStatus(path).getLen
  }

  def getSizeInMB(fileName: String) = getSizeInBytes(fileName) / (1024l * 1024l)


  val sizeUDF: UserDefinedFunction = udf((fileName: String) => getSizeInMB(fileName))

}

