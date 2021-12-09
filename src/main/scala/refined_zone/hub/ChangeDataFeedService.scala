package com.cxi.cdp.data_processing
package refined_zone.hub

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** A table to track CDF progress looks something like this:
  *
  * CREATE TABLE `cdf_tracker` (
  * `consumer_id` STRING,
  * `table_name` STRING,
  * `latest_processed_version` LONG)
  * USING delta
  * LOCATION '...';
  *
  * TODO: move to `support`
  */
class ChangeDataFeedService(val cdfTrackerTable: String) {

    import ChangeDataFeedService._

    def getLatestProcessedVersion(consumerId: String, table: String)(implicit spark: SparkSession): Option[Long] = {
        import spark.implicits._

        val rows = spark
            .table(cdfTrackerTable)
            .filter($"consumer_id" === consumerId && $"table_name" === table)
            .select($"latest_processed_version")
            .collect

        rows match {
            case Array(singleRow) => Some(singleRow.getAs[Long]("latest_processed_version"))
            case _ => None
        }
    }

    def getLatestAvailableVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        val rows = spark.sql(s"DESCRIBE HISTORY $table")
            .agg(max($"version").as("latest_version"))
            .collect

        rows(0).getAs[Long]("latest_version")
    }

    // TODO: fail if CDF is not enabled?
    def getCDFEnabledVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        val rows = spark.sql(s"DESCRIBE HISTORY $table")
            .filter(get_json_object($"operationParameters.properties", "$['delta.enableChangeDataFeed']") === "true")
            .agg(max($"version").as("version"))
            .collect

        rows(0).getAs[Long]("version")
    }

    def queryChangeData(consumerId: String, table: String)(implicit spark: SparkSession): ChangeDataResult = {
        val latestProcessedVersion = getLatestProcessedVersion(consumerId, table)
        val earliestUnprocessedVersion = latestProcessedVersion.map(_ + 1L).getOrElse(getCDFEnabledVersion(table))

        val latestAvaialbleVersion = getLatestAvailableVersion(table)

        val changeData =
            if (earliestUnprocessedVersion <= latestAvaialbleVersion) {
                val df = spark.read.format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", earliestUnprocessedVersion)
                    .option("endingVersion", latestAvaialbleVersion)
                    .table(table)

                Some(df)
            } else {
                None
            }

        ChangeDataResult(latestProcessedVersion, latestAvaialbleVersion, changeData)
    }

    def setLatestProcessedVersion(updates: CDFTrackerRow*)(implicit spark: SparkSession): Unit = {
        import spark.implicits._

        val updatesDF = updates.toDS
            .withColumnRenamed("consumerId", "consumer_id")
            .withColumnRenamed("table", "table_name")
            .withColumnRenamed("latestProcessedVersion", "latest_processed_version")

        val srcTable = "cdf_tracker_updates"
        updatesDF.createOrReplaceTempView(srcTable)

        spark.sqlContext.sql(
            s"""
                MERGE INTO $cdfTrackerTable
                USING $srcTable
                ON $cdfTrackerTable.consumer_id <=> $srcTable.consumer_id
                  AND $cdfTrackerTable.table_name <=> $srcTable.table_name
                WHEN MATCHED
                  THEN UPDATE SET *
                WHEN NOT MATCHED
                  THEN INSERT *
            """)
    }

}

object ChangeDataFeedService {

    case class ChangeDataResult(
                                   latestProcessedVersion: Option[Long],
                                   latestAvailableVersion: Long,
                                   changeData: Option[DataFrame])

    case class CDFTrackerRow(
                                consumerId: String,
                                table: String,
                                latestProcessedVersion: Long)

}
