package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/** Provides service methods to work with Databricks ChangeDataFeed (CDF) feature.
  *
  * Each delta table tracks its versions, regardless of whether CDF was enabled for this table or not.
  * Every change to a delta table creates a new version of this table.
  *
  * If CDF is enabled for a table, we can query row changes between different versions of this table
  * (but only for versions after the version when CDF was enabled).
  *
  * The proposed solution works quite similar to Kafka. Each consumer (a consumer can be a specific Spark job,
  * or some other service) tracks latest versions that it has processed for each table the consumer is using.
  * The latest versions for each consumer / table are stored in a service table `cdfTrackerTable`.
  *
  * When a consumer wants to read the change data for a specific table since its last processing time,
  * all it needs to do is get the latest available version for that table, get the latest processed version
  * for the consumer / table, and read row changes between these versions.
  */
class ChangeDataFeedService(val cdfTrackerTable: String) {

    /** Returns true if ChangeDataFeed is currently enabled for the specified table. */
    def isCdfEnabled(table: String)(implicit spark: SparkSession): Boolean = {
        import spark.implicits._

        val rows = spark.sql(s"SHOW TBLPROPERTIES $table")
            .filter($"key" === "delta.enableChangeDataFeed" && $"value" === "true")

        !rows.isEmpty
    }

    /** Returns the latest table version processed by a consumer with the specified consumerId. */
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

    /** Returns the latest version for the specified table. */
    def getLatestAvailableVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        // even a newly created table has one version, so we should get at least one row
        spark.sql(s"DESCRIBE HISTORY $table")
            .agg(max($"version"))
            .as[Long]
            .head
    }

    /** Returns the version of the specified table when ChangeDataFeed was enabled.
      *
      * The specified table should have CDF enabled, otherwise an exception would be thrown.
      * CDF can be enabled any time.
      * If CDF was enabled at the table creation time, the returned version is 0.
      */
    def getEarliestCdfEnabledVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        ensureCdfEnabled(table)

        // as CDF is enabled, we should get at least one row
        spark.sql(s"DESCRIBE HISTORY $table")
            .filter(get_json_object($"operationParameters.properties", "$['delta.enableChangeDataFeed']") === "true")
            .agg(max($"version"))
            .as[Long]
            .head
    }

    /** Returns change data for the specified consumer/table as a DataFrame along with some metadata. */
    def queryChangeData(consumerId: String, table: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
        ensureCdfEnabled(table)

        val latestProcessedVersion = getLatestProcessedVersion(consumerId, table)
        val earliestUnprocessedVersion = latestProcessedVersion match {
            case Some(version) => version + 1L
            case None => getEarliestCdfEnabledVersion(table)
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

    /** Updates latest processed versions in the CDF Tracker table. */
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
