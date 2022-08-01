package com.cxi.cdp.data_processing
package raw_zone.file_ingestion_framework

import raw_zone.pos_parbrink.udf.ParbrinkUdfs

import org.apache.spark.sql.{DataFrame, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructType}

object FileIngestionFrameworkTransformations {
    def transformationFunctionsMap: Map[String, DataFrame => DataFrame] = Map[String, DataFrame => DataFrame](
        "identity" -> identity,
        "spaceToUnderscore" -> spaceToUnderScoreInColumnNamesTransformation,
        "transformOracleSim" -> transformOracleSim,
        "transformQuBeyond" -> transformQuBeyond,
        "transformSquare" -> transformSquare,
        "transformToast" -> transformToast,
        "transformSegmint" -> transformSegmint,
        "transformVeraset" -> transformVeraset,
        "transformParbrink" -> transformParbrink
    )

    final val CxiCommonColumns: List[String] = List("feed_date", "file_name", "cxi_id")

    /** @author - Luis Velez
      * @createdOn - Date
      * @version - 1.0
      * @Ticket - N/A
      * @App-Dependency - N/A
      * @function-desc - Generic Transformation (no changes)
      */
    def identity(df: DataFrame): DataFrame = {
        df
    }

    /** @author - Luis Velez
      * @createdOn - 2/6/2022
      * @version - 1.0
      * @Ticket - 1404
      * @App-Dependency - N/A
      * @function-desc - Simple Transformation for Segmint data
      */
    def transformSegmint(df: DataFrame): DataFrame = {
        val segmintColPerType = df.columns.filter(col => !CxiCommonColumns.contains(col))
        val substrStart = "202102_cxi_".length() + 1
        val recordTypeLength = "202102_cxi_".length() + ".csv.gz".length()

        df
            .withColumn(
                "record_type",
                expr(s"substring(file_name, ${substrStart}, length(file_name)- ${recordTypeLength})")
            )
            .withColumn(
                "record_value",
                coalesce(segmintColPerType.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*)
            )
            .select("record_type", "record_value", "feed_date", "file_name", "cxi_id")
    }

    /** @author - Mark Norkin
      * @createdOn - 10/08/2021
      * @version - 1.0
      * @Ticket - 120
      * @App-Dependency - Oracle sim Integration
      * @function-desc - Generic Transformation (no changes)
      */
    def transformOracleSim(df: DataFrame): DataFrame = {
        val oracleCommonColumns: List[String] = List("curUTC", "locRef", "busDt", "latestBusDt", "opnBusDt")

        val oracleSimColPerType =
            df.columns.filter(col => !oracleCommonColumns.contains(col) && !CxiCommonColumns.contains(col))

        transformCompositeColumns(df, oracleSimColPerType)
            .withColumn(
                "record_type",
                coalesce(oracleSimColPerType.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))): _*)
            )
            .withColumn(
                "record_value",
                coalesce(oracleSimColPerType.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*)
            )
            .withColumnRenamed("curUTC", "cur_utc")
            .withColumnRenamed("locRef", "loc_ref")
            .withColumnRenamed("busDt", "bus_dt")
            .withColumnRenamed("opnBusDt", "opn_bus_dt")
            .withColumnRenamed("latestBusDt", "latest_bus_dt")
            .select(
                "cur_utc",
                "loc_ref",
                "bus_dt",
                "opn_bus_dt",
                "latest_bus_dt",
                "record_type",
                "record_value",
                "feed_date",
                "cxi_id",
                "file_name"
            )
    }

    def transformQuBeyond(df: DataFrame): DataFrame = {
        val quBeyondCommonColumns: List[String] =
            List(
                "req_customer_id",
                "req_location_id",
                "req_data_type",
                "req_sub_data_type",
                "data_delta",
                "req_start_date",
                "req_end_date"
            )

        val dfData = df.select((List("data.*") ++ CxiCommonColumns ++ quBeyondCommonColumns).map(col).toArray: _*)

        val quBeyondColPerType =
            dfData.columns.filter(col => !quBeyondCommonColumns.contains(col) && !CxiCommonColumns.contains(col))

        transformCompositeColumns(dfData, quBeyondColPerType)
            .withColumn(
                "record_type",
                coalesce(quBeyondColPerType.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))): _*)
            )
            .withColumn(
                "record_value",
                coalesce(quBeyondColPerType.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*)
            )
            .select(
                (List("record_type", "record_value") ++ CxiCommonColumns ++ quBeyondCommonColumns).map(col).toArray: _*
            )
    }

    def spaceToUnderScoreInColumnNamesTransformation(df: DataFrame): DataFrame = {
        val colsRenamed = df.columns.zip(df.columns.map(col => col.replace(' ', '_'))).map(el => col(el._1).as(el._2))
        df.select(colsRenamed: _*)
    }

    def transformSquare(df: DataFrame): DataFrame = {

        val cursorColumn: String = "cursor"

        val squareColPerType =
            df.columns
                .filter(!CxiCommonColumns.contains(_))
                .filter(!_.equals(cursorColumn))

        val outputColumns = ("record_type" :: "record_value" :: CxiCommonColumns).map(col(_))

        transformCompositeColumns(df, squareColPerType)
            .withColumn(
                "record_type",
                coalesce(squareColPerType.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))): _*)
            )
            .withColumn(
                "record_value",
                coalesce(squareColPerType.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*)
            )
            .select(outputColumns: _*)
    }

    def transformParbrink(df: DataFrame): DataFrame = {

        val singularDataTypes = Seq("options")

        val enrichedDf = df
            .withColumn("record_type", ParbrinkUdfs.recordTypeByFilePath(col("file_name")))
            .withColumn("location_id", ParbrinkUdfs.locationByFilePath(col("file_name")))

        val parbrinkRecordTypes = enrichedDf
            .select("record_type")
            .distinct()
            .map(r => r.getString(0))(Encoders.STRING)
            .collect()

        val byRecordTypeDf = parbrinkRecordTypes.foldLeft(enrichedDf)((acc, recordType) => {
            val allRecordsByType =
                df.sparkSession.read.json(
                    enrichedDf.select("value").as[String](Encoders.STRING).where(col("record_type") === recordType)
                )
            val dataType = if (singularDataTypes.contains(recordType)) {
                allRecordsByType.schema
            } else {
                DataTypes.createArrayType(allRecordsByType.schema)
            }
            acc.withColumn(
                recordType,
                when(col("record_type") === lit(recordType), from_json(col("value"), dataType)).otherwise(null)
            )
        })

        val transformedDf = transformCompositeColumns(byRecordTypeDf, parbrinkRecordTypes)

        val outputColumns = ("record_type" :: "record_value" :: "location_id" :: CxiCommonColumns).map(col(_))

        transformedDf
            .withColumn(
                "record_value",
                coalesce(parbrinkRecordTypes.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*)
            )
            .select(outputColumns: _*)

    }

    def transformVeraset(df: DataFrame): DataFrame = {
        val aaidPlatformName = "aaid"
        val idfaPlatformName = "idfa"
        val isIpv6Address = col("ip_address").contains(":")

        df
            .withColumn(
                "advertiser_id_AAID",
                when(col("id_type") === aaidPlatformName, col("ad_id")).otherwise(lit(null))
            )
            .withColumn(
                "advertiser_id_IDFA",
                when(col("id_type") === idfaPlatformName, col("ad_id")).otherwise(lit(null))
            )
            .withColumn(
                "advertiser_id_UNKNOWN",
                when(
                    col("id_type").notEqual(lit(idfaPlatformName)) and col("id_type").notEqual(lit(aaidPlatformName)),
                    col("ad_id")
                ).otherwise(lit(null))
            )
            .withColumn("ipv_4", when(isIpv6Address, lit(null)).otherwise(col("ip_address")))
            .withColumn("ipv_6", when(isIpv6Address, col("ip_address")).otherwise(lit(null)))
            .withColumn("id_type", upper(col("id_type")))
            .drop("ad_id", "ip_address")
    }

    // scalastyle:off method.length
    def transformToast(df: DataFrame): DataFrame = {
        val recordTypeFolderNameElementPosition = 7
        val recordTypeUdf = udf((fileName: String) => {
            val folderParts = fileName.split("/")
            folderParts(recordTypeFolderNameElementPosition)
        })

        val restaurantFolderNameElementPositionForOrders = 8
        val orderLocationIdUdf = udf((fileName: String) => {
            val restaurantPartition = fileName.split("/")(restaurantFolderNameElementPositionForOrders)
            restaurantPartition.replace("restaurant=", "")
        })

        val enrichedDf = df
            .withColumn("record_type", recordTypeUdf(col("file_name")))
            .withColumn(
                "location_id",
                when(col("record_type") === "orders", orderLocationIdUdf(col("file_name"))).otherwise(lit(null))
            )

        val toastRecordTypesExceptMenus = enrichedDf
            .select("record_type")
            .distinct()
            .map(r => r.getString(0))(Encoders.STRING)
            .collect()
            .filter(rt => !rt.equals("menus"))

        val singularDataTypes = Seq("restaurants")

        val byRecordTypeDfExceptMenus = toastRecordTypesExceptMenus
            .foldLeft(enrichedDf.filter(col("record_type") =!= "menus"))((acc, recordType) => {
                val recordTypeDf =
                    df.sparkSession.read.json(
                        enrichedDf.select("value").as[String](Encoders.STRING).where(col("record_type") === recordType)
                    )
                val dataType = if (singularDataTypes.contains(recordType)) {
                    recordTypeDf.schema
                } else {
                    DataTypes.createArrayType(recordTypeDf.schema)
                }
                acc.withColumn(
                    recordType,
                    when(col("record_type") === lit(recordType), from_json(col("value"), dataType)).otherwise(null)
                )
            })

        val menusRecordTypeDf = enrichedDf
            .filter(col("record_type") === "menus")

        val transformedCompositeColumns =
            transformCompositeColumns(byRecordTypeDfExceptMenus, toastRecordTypesExceptMenus)

        val outputColumns = ("record_type" :: "record_value" :: "location_id" :: CxiCommonColumns).map(col(_))
        transformedCompositeColumns
            .withColumn(
                "record_value",
                coalesce(toastRecordTypesExceptMenus.map(c => when(col(c).isNotNull, col(c)).otherwise(lit(null))): _*)
            )
            .select(outputColumns: _*)
            .unionByName(menusRecordTypeDf.withColumnRenamed("value", "record_value"))
    }

    private def transformCompositeColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
        val dfWithExplodedArrays = columns.foldLeft(df) { case (acc, col_name) =>
            acc.schema(col_name).dataType match {
                case ArrayType(_, _) => acc.withColumn(col_name, explode_outer(col(col_name)))
                case _ => acc
            }
        }

        val dfWithJsonStructs = columns.foldLeft(dfWithExplodedArrays) { case (acc, col_name) =>
            acc.schema(col_name).dataType match {
                case StructType(_) => acc.withColumn(col_name, to_json(col(col_name)))
                case _ => acc
            }
        }

        dfWithJsonStructs
    }

}
