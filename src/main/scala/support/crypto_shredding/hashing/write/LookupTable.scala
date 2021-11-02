package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.write

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.UUID.randomUUID

class LookupTable(spark: SparkSession, val dbName: String, val tableName: String) extends Serializable {
    private val requiredFields = Seq("process_name", "country", "cxi_partner_id", "original_value", "hashed_value")

    private val generateUUID = udf(() => randomUUID().toString)

    def upsert(df: DataFrame): Unit = {
        if (!checkRequiredFields(df)) {
            throw new IllegalArgumentException(s"There aren't required fields (${requiredFields.mkString(", ")}) in input dataframe.")
        }

        val dfToWrite = df
            .select(requiredFields.map(col):_*)
            .withColumn("feed_date", current_timestamp()) // TODO: discuss the use case, not idempotent
            .withColumn("id", generateUUID()) // TODO: discuss the use case, not idempotent
            .dropDuplicates("country", "cxi_partner_id", "hashed_value")

        val srcTable = "newPrivateInfo"

        dfToWrite.createOrReplaceTempView(srcTable)

        spark.sql(
            s"""
               |MERGE INTO $dbName.$tableName
               |USING $srcTable
               |ON $dbName.$tableName.country <=> $srcTable.country
               | AND $dbName.$tableName.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $dbName.$tableName.hashed_value <=> $srcTable.hashed_value
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    private def checkRequiredFields(df: DataFrame): Boolean = {
        df.columns.intersect(requiredFields).length == requiredFields.length
    }
}
