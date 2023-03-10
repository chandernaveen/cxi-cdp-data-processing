package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.spark.sql.SparkSession

trait ChangeDataFeedSource {

    protected def cdfService: ChangeDataFeedService

    /** Returns data changed between the last processed version and the latest available version for this consumer ID */
    def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult

    /** Returns all data for this source.
      *
      * The difference between this method and a simple table query is that we get table versions as well, thus
      * we can reliably commit table versions for this consumer after we've finished the processing.
      *
      * The main use case is the following:
      * 1. Use `queryChangeData` for usual incremental processing - process only newly added/updated data.
      * 2. If using `queryChangeData` is not possible (e.g. a lot of data was changed in a way that cannot be
      * incrementally reprocessed), use `queryAllData` to reprocess everything from the start.
      */
    def queryAllData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult

    def markProcessed(result: ChangeDataQueryResult)(implicit spark: SparkSession): Unit = {
        val updates = result.tableMetadataSeq.map(m => CdfTrackerTableVersion(m.table, m.endVersion))
        cdfService.setLatestProcessedVersion(result.consumerId, updates: _*)
    }

}

object ChangeDataFeedSource {

    /** CDF source for a single table. */
    class SingleTable(protected val cdfService: ChangeDataFeedService, table: String) extends ChangeDataFeedSource {

        override def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            cdfService.queryChangeData(consumerId, table)
        }

        override def queryAllData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            val latestAvailableVersion = cdfService.getLatestAvailableVersion(table)

            val df = spark.read
                .format("delta")
                .option("versionAsOf", latestAvailableVersion)
                .table(table)

            ChangeDataQueryResult(
                consumerId = consumerId,
                tableMetadataSeq = Seq(
                    ChangeDataQueryResult
                        .TableMetadata(table = table, startVersion = 0L, endVersion = latestAvailableVersion)
                ),
                data = Some(df)
            )
        }

    }

    /** CDF source for a simple union of tables. All tables must have the same schema. */
    class SimpleUnion(protected val cdfService: ChangeDataFeedService, tables: Seq[String])
        extends ChangeDataFeedSource {

        private val tableSources = tables.map(table => new SingleTable(cdfService, table))

        override def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            verifyUnionIsSafe(tables)

            tableSources
                .map(_.queryChangeData(consumerId))
                .reduce(union)
        }

        override def queryAllData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            verifyUnionIsSafe(tables)

            tableSources
                .map(_.queryAllData(consumerId))
                .reduce(union)
        }

        private def verifyUnionIsSafe(tables: Seq[String])(implicit spark: SparkSession): Unit = {
            val schemas = tables.map(table => spark.table(table).schema)
            schemas match {
                case Seq() => ()
                case firstSchema +: restSchemas =>
                    val allSchemasAreEqual = restSchemas.forall(schema => schema == firstSchema)
                    if (!allSchemasAreEqual) {
                        throw new IllegalArgumentException(s"Tables $tables do not have the same schema")
                    }
            }
        }

        private def union(first: ChangeDataQueryResult, second: ChangeDataQueryResult): ChangeDataQueryResult = {
            val combinedTableMetadataSeq = first.tableMetadataSeq ++ second.tableMetadataSeq

            val combinedChangeData = (first.data, second.data) match {
                case (Some(a), Some(b)) => Some(a.union(b))
                case _ => first.data.orElse(second.data)
            }

            first.copy(tableMetadataSeq = combinedTableMetadataSeq, data = combinedChangeData)
        }
    }

}
