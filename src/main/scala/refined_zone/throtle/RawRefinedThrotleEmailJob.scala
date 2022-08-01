package com.cxi.cdp.data_processing
package refined_zone.throtle
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import com.cxi.cdp.data_processing.support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.array_distinct

import java.nio.file.Paths

object RawRefinedThrotleEmailJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()
        val contractPath = "/mnt/" + args(0)
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))
        val feedDate = args(1)

        run(spark, contract, feedDate)
    }

    def run(spark: SparkSession, contract: ContractUtils, feedDate: String): Unit = {

        val srcDbName = contract.prop[String]("schema.raw.db_name")
        val srcTable = contract.prop[String]("schema.raw.data_table")
        val refinedThrotleDb = contract.prop[String]("schema.refined_throtle.db_name")
        val throtleEmailTable = contract.prop[String]("schema.refined_throtle.throtle_email_table")

        val throtleEmailRawDf = readThrotleEmailRaw(s"$srcDbName.$srcTable", feedDate, spark)
        val throtleEmailTransformDf = transformThrotleEmail(throtleEmailRawDf)
        writeThrotleEmails(throtleEmailTransformDf, s"$refinedThrotleDb.$throtleEmailTable")
    }

    def readThrotleEmailRaw(srcTable: String, feedDate: String, spark: SparkSession): DataFrame = {

        spark
            .table(srcTable)
            .where(col("feed_date") === feedDate and col("sha256_lower_email").isNotNull)
            .drop("feed_date", "file_name", "cxi_id")
            .dropDuplicates("throtle_id", "sha256_lower_email")

    }

    def transformThrotleEmail(throtleEmailRawDf: DataFrame): DataFrame = {

        throtleEmailRawDf
            .groupBy("throtle_id")
            .agg(
                max("throtle_hhid") as "throtle_hhid",
                collect_set("sha256_lower_email") as "sha256_lower_emails"
            )

    }

    def writeThrotleEmails(refinedDf: DataFrame, destTable: String): Unit = {
        val srcTable = "newTIDEmail"

        refinedDf.createOrReplaceTempView(srcTable)
        refinedDf.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.throtle_id <=> $srcTable.throtle_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
