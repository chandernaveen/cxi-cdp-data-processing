package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import org.apache.spark.sql.DataFrame

object WriteOptionsFunctions {
    type WriteOptionsFunction = (DataFrame, Map[String, String]) => Map[String, String]

    def getWriteOptions(df: DataFrame, feedDate: String, config: FileIngestionFrameworkConfig): Map[String, String] = {
        val functionsMap: Map[String, WriteOptionsFunction] =
            Map(
                "replaceWhereForSingleColumn" -> replaceWhereForSingleColumn(),
                "replaceWhereForFeedDate" -> replaceWhereForFeedDate(feedDate)
            )

        config.writeOptionsFunctionName match {
            case None => config.writeOptions
            case Some(functionName) =>
                functionsMap(functionName)(df, config.writeOptionsFunctionParams) ++ config.writeOptions
        }
    }

    def replaceWhereForSingleColumn(): WriteOptionsFunction = (df: DataFrame, params: Map[String, String]) => {
        val controlCol = params("controlCol")

        val controlColValuesProcessed = df
            .select(controlCol)
            .distinct
            .collect
            .map(_(0))
            .map(r => s"'$r'")
            .mkString(",")

        Map("replaceWhere" -> s"$controlCol in ($controlColValuesProcessed)")
    }

    /** Creates WriteOptionsFunction to only overwrite records with the provided feedDate.
      *
      * Without this ingesting data for a specific feedDate will overwrite data for previously imported feedDates.
      * Should be used together with SaveMode.Overwrite.
      *
      * Using this function instead of `replaceWhereForSingleColumnWriteOption` helps to avoid a full scan before write.
      */
    def replaceWhereForFeedDate(feedDate: String): WriteOptionsFunction = (_, _) => {
        Map("replaceWhere" -> s"feed_date = '$feedDate'")
    }

}
