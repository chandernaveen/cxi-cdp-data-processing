package com.cxi.cdp.data_processing
package support.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object TransformUtils {

    val SourceCol = "source_col"
    val DestCol = "dest_col"
    val CastDataType = "cast_data_type"

    def applyColumnMapping(sourceDf: DataFrame, columnsMapping: ColumnsMapping, includeAllSourceColumns: Boolean): DataFrame = {
        val selectedCollectionColumns =
            if (includeAllSourceColumns) sourceDf else sourceDf.select(columnsMapping.sourceColumns.map(col): _*)

        columnsMapping.mappings.foldLeft(selectedCollectionColumns) {
            case (acc, colMapping) =>
                val renamedColumn = acc
                    .withColumnRenamed(colMapping.sourceColName, colMapping.destColName)

                colMapping.castDataType
                    .map(dt => renamedColumn.withColumn(colMapping.destColName, col(colMapping.destColName).cast(dt)))
                    .getOrElse(renamedColumn)
        }

    }

    case class ColumnsMapping(colConfigs: Seq[Map[String, String]]) {
        val mappings: Seq[ColumnMapping] = colConfigs.map(colConfig =>
            ColumnMapping(colConfig(SourceCol), colConfig(DestCol), colConfig.get(CastDataType)))
        val sourceColumns: Seq[String] = mappings.map(_.sourceColName)
    }

    case class ColumnMapping(sourceColName: String, destColName: String, castDataType: Option[String])

}
