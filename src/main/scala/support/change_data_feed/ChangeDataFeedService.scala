package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class ChangeDataFeedService(val cdfTrackerTable: String) {

    def isCdfEnabled(table: String)(implicit spark: SparkSession): Boolean = {
        import spark.implicits._

        val rows = spark.sql(s"SHOW TBLPROPERTIES $table")
            .filter($"key" === "delta.enableChangeDataFeed" && $"value" === "true")

        !rows.isEmpty
    }

    def getLatestProcessedVersion(consumerId: String, table: String)(implicit spark: SparkSession): Option[Long] = {
        import spark.implicits._

        spark
            .table(cdfTrackerTable)
            .as[CdfTrackerRow]
            .filter(r => r.consumer_id == consumerId && r.table_name == table)
            .map(_.latest_processed_version)
            .collect
            .headOption
    }

    def getLatestAvailableVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        // even a newly created table has one version, so we should get at least one row
        spark.sql(s"DESCRIBE HISTORY $table")
            .agg(max($"version"))
            .as[Long]
            .head
    }

    def getCdfEnabledVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        ensureCdfEnabled(table)

        // as CDF is enabled, we should get at least one row
        spark.sql(s"DESCRIBE HISTORY $table")
            .filter(get_json_object($"operationParameters.properties", "$['delta.enableChangeDataFeed']") === "true")
            .agg(max($"version"))
            .as[Long]
            .head
    }

    def queryChangeData(consumerId: String, table: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
        ensureCdfEnabled(table)

        val latestProcessedVersion = getLatestProcessedVersion(consumerId, table)
        val earliestUnprocessedVersion = latestProcessedVersion match {
            case Some(version) => version + 1L
            case None => getCdfEnabledVersion(table)
        }
        val latestAvailableVersion = getLatestAvailableVersion(table)

        val changeData =
            if (earliestUnprocessedVersion <= latestAvailableVersion) {
                val df = spark.read.format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", earliestUnprocessedVersion)
                    .option("endingVersion", latestAvailableVersion)
                    .table(table)

                Some(df)
            } else {
                None
            }

        val tableMetadata = ChangeDataQueryResult.TableMetadata(
            table,
            latestProcessedVersion,
            earliestUnprocessedVersion,
            latestAvailableVersion)

        ChangeDataQueryResult(consumerId, List(tableMetadata), changeData)
    }

    def setLatestProcessedVersion(updates: CdfTrackerRow*)(implicit spark: SparkSession): Unit = {
        import spark.implicits._

        val srcTable = "cdf_tracker_updates"
        updates.toDS.createOrReplaceTempView(srcTable)

        spark.sql(
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

    private def ensureCdfEnabled(table: String)(implicit spark: SparkSession): Unit = {
        if (!isCdfEnabled(table)) {
            throw new IllegalArgumentException(s"ChangeDataFeed is not enabled for $table")
        }
    }

}
