package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.spark.sql.SparkSession

trait ChangeDataFeedSource {

    protected def cdfService: ChangeDataFeedService

    def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult

    def markProcessed(result: ChangeDataQueryResult)(implicit spark: SparkSession): Unit = {
        val cdfTrackerUpdates = result.tableMetadataSeq
            .map(m => CdfTrackerRow(result.consumerId, m.table, m.latestAvailableVersion))
        cdfService.setLatestProcessedVersion(cdfTrackerUpdates: _*)
    }

}

object ChangeDataFeedSource {

    /** CDF source for a single table. */
    class SingleTable(protected val cdfService: ChangeDataFeedService, table: String) extends ChangeDataFeedSource {
        def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            cdfService.queryChangeData(consumerId, table)
        }
    }

    /** CDF source for a simple union of tables. All tables must have the same schema. */
    class SimpleUnion(protected val cdfService: ChangeDataFeedService, tables: Seq[String]) extends ChangeDataFeedSource {
        override def queryChangeData(consumerId: String)(implicit spark: SparkSession): ChangeDataQueryResult = {
            verifyUnionIsSafe(tables)

            val queryResults = tables.map(table => cdfService.queryChangeData(consumerId, table))
            queryResults.reduce((acc, result) => {
                    val combinedTableMetadataSeq = acc.tableMetadataSeq ++ result.tableMetadataSeq
                    val combinedChangeData = (acc.changeData, result.changeData) match {
                        case (Some(a), Some(b)) => Some(a.union(b))
                        case _ => acc.changeData.orElse(result.changeData)
                    }

                    acc.copy(tableMetadataSeq = combinedTableMetadataSeq, changeData = combinedChangeData)
            })
        }

        private def verifyUnionIsSafe(tables: Seq[String])(implicit spark: SparkSession): Unit = {
            val schemas = tables.map(table => spark.table(table).schema)
            schemas match {
                case Seq() => ()
                case firstSchema +: restSchemas =>
                    val allSchemasAreEqual = restSchemas.forall(schema => schema == firstSchema)
                    if (!allSchemasAreEqual) {
                        throw new IllegalArgumentException(s"Tables $tables are not of the same schema")
                    }
            }
        }
    }

}
