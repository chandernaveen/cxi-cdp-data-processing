package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

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

    private val logger = Logger.getLogger(this.getClass.getName)

    /** Returns true if ChangeDataFeed is currently enabled for the specified table. */
    def isCdfEnabled(table: String)(implicit spark: SparkSession): Boolean = {
        import spark.implicits._

        val rows = spark
            .sql(s"SHOW TBLPROPERTIES $table")
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
        spark
            .sql(s"DESCRIBE HISTORY $table")
            .agg(max($"version"))
            .as[Long]
            .head
    }

    /** Returns the earliest version for the specified table. */
    def getEarliestAvailableVersion(table: String)(implicit spark: SparkSession): Long = {
        import spark.implicits._

        // even a newly created table has one version, so we should get at least one row
        spark
            .sql(s"DESCRIBE HISTORY $table")
            .agg(min($"version"))
            .as[Long]
            .head
    }

    /** Returns the version of the specified table when ChangeDataFeed was enabled.
      *
      * The specified table should have CDF enabled, otherwise an exception would be thrown.
      * CDF can be enabled any time.
      * If CDF was enabled at the table creation time, the returned version is 0.
      */
    def getEarliestCdfEnabledVersion(table: String)(implicit spark: SparkSession): Option[Long] = {
        import spark.implicits._

        ensureCdfEnabled(table)

        spark
            .sql(s"DESCRIBE HISTORY $table")
            .filter(get_json_object($"operationParameters.properties", "$['delta.enableChangeDataFeed']") === "true")
            .agg(max($"version"))
            .as[Option[Long]]
            .head
    }

    /** Returns change data for the specified consumer/table as a DataFrame along with some metadata. */
    def queryChangeData(consumerId: String, table: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
        ensureCdfEnabled(table)

        val startVersion = getStartVersion(consumerId, table)
        val endVersion = getLatestAvailableVersion(table)

        val changeData =
            if (startVersion <= endVersion) {
                val df = spark.read
                    .format("delta")
                    .option("readChangeFeed", "true")
                    .option("startingVersion", startVersion)
                    .option("endingVersion", endVersion)
                    .table(table)

                Some(df)
            } else {
                None
            }

        val tableMetadata =
            ChangeDataQueryResult.TableMetadata(table = table, startVersion = startVersion, endVersion = endVersion)

        ChangeDataQueryResult(consumerId, List(tableMetadata), changeData)
    }

    /** Returns the start version for CDF query for the provided consumer/table.
      * Throws an exception if not all versions starting from the start version exist.
      */
    private[change_data_feed] def getStartVersion(consumerId: String, table: String)(implicit
        spark: SparkSession
    ): Long = {
        getLatestProcessedVersion(consumerId, table)
            .map(version => version + 1L)
            .orElse(getEarliestCdfEnabledVersion(table))
            .filter(nextVersion => getEarliestAvailableVersion(table) <= nextVersion)
            .getOrElse {
                val errorMessage = s"ChangeDataFeed is enabled for [$table] but some table versions not yet " +
                    s"processed by $consumerId are no longer available. Consider full reprocess to avoid losing data."
                throw new Exception(errorMessage)
            }
    }

    /** Updates latest processed versions in the CDF Tracker table. */
    def setLatestProcessedVersion(consumerId: String, updates: CdfTrackerTableVersion*)(implicit
        spark: SparkSession
    ): Unit = {
        import spark.implicits._

        val srcTable = "cdf_tracker_updates"
        updates.toDS.createOrReplaceTempView(srcTable)

        spark.sql(s"""
                MERGE INTO $cdfTrackerTable
                USING $srcTable
                ON $cdfTrackerTable.consumer_id <=> '$consumerId'
                  AND $cdfTrackerTable.table_name <=> $srcTable.table_name
                WHEN MATCHED
                  THEN UPDATE SET
                    latest_processed_version = $srcTable.latest_processed_version
                WHEN NOT MATCHED
                  THEN INSERT (
                    consumer_id,
                    table_name,
                    latest_processed_version
                  ) VALUES(
                    '$consumerId',
                    $srcTable.table_name,
                    $srcTable.latest_processed_version
                  )
            """)
    }

    private def ensureCdfEnabled(table: String)(implicit spark: SparkSession): Unit = {
        if (!isCdfEnabled(table)) {
            throw new IllegalArgumentException(s"ChangeDataFeed is not enabled for $table")
        }
    }

}

object ChangeDataFeedService {

    final val ChangeTypeColumnName = "_change_type"

    final val CommitVersionColumnName = "_commit_version"

    final val CommitTimestampColumnName = "_commit_timestamp"

    val CdfColumnNames = Seq(
        ChangeTypeColumnName,
        CommitVersionColumnName,
        CommitTimestampColumnName
    )

    /** Change Data Feed result will have additional fields, one of them is `_change_type`.
      * It shows how exactly this particular record was changed:
      * 1. If it is a new record, its change type is `insert`
      * 2. If the record was updated, we will get two records: one with the change type of `update_preimage`,
      *    showing the record before the update, and the other with the change type of `update_postimage`,
      *    showing the record after the update.
      * 3. If the record was deleted, we will get this record with the change type of `deleted`.
      */
    object ChangeType {
        final val Insert = "insert"
        final val UpdatePreImage = "update_preimage"
        final val UpdatePostImage = "update_postimage"
        final val Delete = "delete"
    }

    def getCommitVersion(row: Row): Long = {
        row.getAs[Long](CommitVersionColumnName)
    }

    def getChangeType(row: Row): String = {
        row.getAs[String](ChangeTypeColumnName)
    }

}
