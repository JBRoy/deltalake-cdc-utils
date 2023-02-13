package com.zeotap.deltalake.cdc

import com.zeotap.deltalake.cdc.CDCOps.getTimestampByPartitions
import com.zeotap.deltalake.cdc.CDFConstants.DeltaPath
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.LocalDateTime

object ChangeDataFeedUtils {
  implicit class CDF(deltaPath: DeltaPath) {
    def getChangeDataBetween(startingDateTime: String, endingDateTime: String)(implicit spark: SparkSession) = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val startingDate = dateFormat.parse(startingDateTime)
      val endingDate = dateFormat.parse(endingDateTime)
      getTimestampByPartitions(deltaPath)
        .filter(s"timestamp >= ${startingDate.getTime} and timestamp <= ${endingDate.getTime}")
    }

    def getChangeDataBetween(startingTimestamp: Long, endingTimeStamp: Long)(implicit spark: SparkSession) = getTimestampByPartitions(deltaPath)
      .filter(s"timestamp >= ${startingTimestamp} and timestamp <= ${endingTimeStamp}")


    def getLatestChangeData(startingTimestamp: String)(implicit spark: SparkSession) = getTimestampByPartitions(deltaPath)
      .filter(s"timestamp >= $startingTimestamp and timestamp <= ${LocalDateTime.now()}")
  }
}
