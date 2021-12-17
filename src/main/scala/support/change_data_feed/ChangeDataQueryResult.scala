package com.cxi.cdp.data_processing
package support.change_data_feed

import org.apache.spark.sql.DataFrame

case class ChangeDataQueryResult(
                                    consumerId: String,
                                    tableMetadataSeq: Seq[ChangeDataQueryResult.TableMetadata],
                                    changeData: Option[DataFrame]
                                )

object ChangeDataQueryResult {

    case class TableMetadata(
                                table: String,
                                latestProcessedVersion: Option[Long],
                                earliestUnprocessedVersion: Long,
                                latestAvailableVersion: Long)

}
