package com.cxi.cdp.data_processing
package refined_zone.throtle
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import com.cxi.cdp.data_processing.support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.Paths

object RawRefinedThrotleMaidJob {

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
        val throtleTable = contract.prop[String]("schema.refined_throtle.throtle_table")

        val throtleRawDf = readThrotleMaidRawTable(s"$srcDbName.$srcTable", feedDate, spark)
        val throtleRefinedTransformDf = transformThrotleMaidRaw(throtleRawDf)
        writeThrotleMaids(throtleRefinedTransformDf, s"$refinedThrotleDb.$throtleTable")
    }

    def readThrotleMaidRawTable(srcTable: String, feedDate: String, spark: SparkSession): DataFrame = {
        spark
            .table(srcTable)
            .where(col("feed_date") === feedDate and col("native_maid").isNotNull)
            .drop("feed_date", "file_name", "cxi_id")
            .dropDuplicates("throtle_id", "native_maid")
    }

    def transformThrotleMaidRaw(throtleRawDf: DataFrame): DataFrame = {
        throtleRawDf
            .groupBy("throtle_id")
            .agg(
                max("throtle_hhid") as "throtle_hhid",
                collect_set("native_maid") as "maids"
            )
    }

    def writeThrotleMaids(refinedDf: DataFrame, destTable: String): Unit = {
        val srcTable = "throtleTable"

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
