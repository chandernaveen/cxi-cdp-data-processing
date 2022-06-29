package com.cxi.cdp.data_processing
package support.crypto_shredding.hashing.write

import support.crypto_shredding.hashing.function_types.CryptoHashingResult
import support.normalization.DateNormalizationUdfs.parseToSqlDateIsoFormat

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

class LookupTable(spark: SparkSession, val dbName: String, val tableName: String) extends Serializable {

    def upsert(df: Dataset[CryptoHashingResult], cxiSource: String, dateRaw: String, runId: String): Unit = {

        val dfToWrite = df
            .dropDuplicates("hashed_value", "identity_type")
            .withColumn("feed_date", parseToSqlDateIsoFormat(lit(dateRaw)))
            .withColumn("run_id", lit(runId))
            .withColumn("cxi_source", lit(cxiSource))

        val srcTable = "newPrivateInfo"

        dfToWrite.createOrReplaceTempView(srcTable)

        spark.sql(s"""
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
}
