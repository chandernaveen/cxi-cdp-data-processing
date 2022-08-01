package com.cxi.cdp.data_processing
package refined_zone.throtle
import com.cxi.cdp.data_processing.support.normalization.udf.LocationNormalizationUdfs.normalizeZipCode
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import com.cxi.cdp.data_processing.support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.array_distinct

import java.nio.file.Paths

object RawRefinedThrotleTidGeoJob {

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
        val throtleGeoTable = contract.prop[String]("schema.refined_throtle.throtle_geo_table")

        val throtleGeoRawDf = readThrotleGeoRaw(s"$srcDbName.$srcTable", feedDate, spark)
        val throtleGeoTransformDf = transformThrotleGeo(throtleGeoRawDf)
        writeThrotleGeos(throtleGeoTransformDf, s"$refinedThrotleDb.$throtleGeoTable")
    }

    def readThrotleGeoRaw(srcTable: String, feedDate: String, spark: SparkSession): DataFrame = {

        spark
            .table(srcTable)
            .where(col("feed_date") === feedDate and col("zip").isNotNull)
            .drop("feed_date", "file_name", "cxi_id")
            .dropDuplicates("throtle_id")
    }

    def transformThrotleGeo(throtleGeoRawDf: DataFrame): DataFrame = {

        throtleGeoRawDf
            .withColumn("zip_code", normalizeZipCode(col("zip")))
            .drop("zip")
    }

    def writeThrotleGeos(refinedDf: DataFrame, destTable: String): Unit = {
        val srcTable = "newTIDGeo"

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
