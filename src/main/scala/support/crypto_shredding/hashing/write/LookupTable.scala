package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.write

import support.crypto_shredding.hashing.function_types.CryptoHashingResult

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.UUID.randomUUID

class LookupTable(spark: SparkSession, val dbName: String, val tableName: String) extends Serializable {

    def upsert(df: Dataset[CryptoHashingResult], cxiSource: String, dateRaw: String, runId: String): Unit = {

        val dfToWrite = df
            .dropDuplicates("hashed_value", "identity_type")
            .withColumn("feed_date", lit(dateRaw))
            .withColumn("run_id", lit(runId))
            .withColumn("cxi_source", lit(cxiSource))

        val srcTable = "newPrivateInfo"

        dfToWrite.createOrReplaceTempView(srcTable)

        spark.sql(
            s"""
               |MERGE INTO $dbName.$tableName
               |USING $srcTable
               |ON $dbName.$tableName.feed_date <=> '$dateRaw'
               | AND $dbName.$tableName.run_id <=> '$runId'
               | AND $dbName.$tableName.cxi_source <=> '$cxiSource'
               | AND $dbName.$tableName.identity_type <=> $srcTable.identity_type
               | AND $dbName.$tableName.hashed_value <=> $srcTable.hashed_value
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

//    TODO: remove below code after https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
    private val requiredFieldsOld = Seq("process_name", "country", "original_value", "hashed_value", "identity_type")
    private val generateUUID = udf(() => randomUUID().toString)

    def upsertOld(df: DataFrame, cxiSource: String): Unit = {
        if (!checkRequiredFieldsOld(df)) {
            throw new IllegalArgumentException(s"There aren't required fields (${requiredFieldsOld.mkString(", ")}) in input dataframe.")
        }

        val dfToWrite = df
            .select(requiredFieldsOld.map(col):_*)
            .withColumn("feed_date", current_timestamp()) // TODO: discuss the use case, not idempotent
            .withColumn("id", generateUUID()) // TODO: discuss the use case, not idempotent
            .dropDuplicates("country", "hashed_value", "identity_type")
            .withColumn("cxi_source", lit(cxiSource))

        val srcTable = "newPrivateInfo"

        dfToWrite.createOrReplaceTempView(srcTable)

        // temporarily hardcoded prev table names, code will be removed after https://dev.azure.com/Customerxi/Cloud%20Data%20Platform/_workitems/edit/2352
        val dbName = "privacy"
        val tableName = "lookup_table"

        spark.sql(
            s"""
               |MERGE INTO $dbName.$tableName
               |USING $srcTable
               |ON $dbName.$tableName.country <=> $srcTable.country
               | AND $dbName.$tableName.cxi_source <=> '$cxiSource'
               | AND $dbName.$tableName.hashed_value <=> $srcTable.hashed_value
               | AND $dbName.$tableName.identity_type <=> $srcTable.identity_type
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    private def checkRequiredFieldsOld(df: DataFrame): Boolean = {
        df.columns.intersect(requiredFieldsOld).length == requiredFieldsOld.length
    }
}
