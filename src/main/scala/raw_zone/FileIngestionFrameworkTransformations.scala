package com.cxi.cdp.data_processing
package raw_zone

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}

object FileIngestionFrameworkTransformations {
    def transformationFunctionsMap: Map[String, DataFrame => DataFrame] = Map[String, DataFrame => DataFrame](
        "identity" -> identity,
        "spaceToUnderscore" -> spaceToUnderScoreInColumnNamesTransformation,
        "transformOracleSim" -> transformOracleSim,
        "transformQuBeyond" -> transformQuBeyond
    )

    /**
     * @author - Luis Velez
     * @createdOn - Date
     * @version - 1.0
     * @Ticket - N/A
     * @App-Dependency - N/A
     * @function-desc - Generic Transformation (no changes)
     */
    def identity(df: DataFrame): DataFrame = {
        df
    }

    /**
     * @author - Mark Norkin
     * @createdOn - 10/08/2021
     * @version - 1.0
     * @Ticket - 120
     * @App-Dependency - Oracle sim Integration
     * @function-desc - Generic Transformation (no changes)
     */
    def transformOracleSim(df: DataFrame): DataFrame = {
        import org.apache.spark.sql.functions._
        val oracleCommonColumns: List[String] = List("curUTC", "locRef", "busDt", "latestBusDt", "opnBusDt")
        val cxiCommonColumns: List[String] = List("feed_date", "file_name", "cxi_id")

        val oracleSimColPerType = df.columns.filter(col => !oracleCommonColumns.contains(col) && !cxiCommonColumns.contains(col))

        val df0 = oracleSimColPerType.foldLeft(df) {
            case (acc, col_name) => acc.schema(col_name).dataType match {
                case ArrayType(_, _) => acc.withColumn(col_name, explode_outer(col(col_name)))
                case _ => acc
            }
        }
        val df1 = oracleSimColPerType.foldLeft(df0) {
            case (acc, col_name) => acc.schema(col_name).dataType match {
                case StructType(_) => acc.withColumn(col_name, to_json(col(col_name)))
                case _ => acc
            }
        }

        df1.withColumn("record_type", coalesce(oracleSimColPerType.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))): _*))
            .withColumn("record_value", coalesce(oracleSimColPerType.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*))
            .withColumnRenamed("curUTC", "cur_utc")
            .withColumnRenamed("locRef", "loc_ref")
            .withColumnRenamed("busDt", "bus_dt")
            .withColumnRenamed("opnBusDt", "opn_bus_dt")
            .withColumnRenamed("latestBusDt", "latest_bus_dt")
            .select("cur_utc", "loc_ref", "bus_dt", "opn_bus_dt", "latest_bus_dt", "record_type", "record_value", "feed_date", "cxi_id", "file_name")
    }

    def transformQuBeyond(df: DataFrame): DataFrame = {
        import org.apache.spark.sql.functions._
        val quBeyondCommonColumns: List[String] = List("req_customer_id", "req_location_id", "req_data_type", "req_sub_data_type", "data_delta", "req_start_date", "req_end_date")
        val cxiCommonColumns: List[String] = List("feed_date", "file_name", "cxi_id")

        val dfData = df.select((List("data.*") ++ cxiCommonColumns ++ quBeyondCommonColumns).map(col).toArray: _*)

        val quBeyondColPerType = dfData.columns.filter(col => !quBeyondCommonColumns.contains(col) && !cxiCommonColumns.contains(col))

        val df0 = quBeyondColPerType.foldLeft(dfData) {
            case (acc, col_name) => acc.schema(col_name).dataType match {
                case ArrayType(_, _) => acc.withColumn(col_name, explode_outer(col(col_name)))
                case _ => acc
            }
        }
        val df1 = quBeyondColPerType.foldLeft(df0) {
            case (acc, col_name) => acc.schema(col_name).dataType match {
                case StructType(_) => acc.withColumn(col_name, to_json(col(col_name)))
                case _ => acc
            }
        }

        df1.withColumn("record_type", coalesce(quBeyondColPerType.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))): _*))
            .withColumn("record_value", coalesce(quBeyondColPerType.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*))
            .select((List("record_type", "record_value") ++ cxiCommonColumns ++ quBeyondCommonColumns).map(col).toArray: _*)
    }

    def spaceToUnderScoreInColumnNamesTransformation(df: DataFrame): DataFrame = {
        val colsRenamed = df.columns.zip(df.columns.map(col => col.replace(' ', '_'))).map(el => col(el._1).as(el._2))
        df.select(colsRenamed: _*)
    }

}
