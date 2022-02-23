package com.cxi.cdp.data_processing
package refined_zone.segmint

import java.nio.file.Paths

import com.cxi.cdp.data_processing.support.{SparkSessionFactory, WorkspaceConfigReader}
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import com.databricks.service.DBUtils
import com.cxi.cdp.data_processing.support.cleansing.udfs.cleanseZipCode
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DoubleType, StringType, IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions.udf
import org.threeten.extra.YearWeek;
import java.time.DayOfWeek;
import java.time.format.DateTimeFormatter;

/** This job parses Raw Segmint Data into a simple Refined copy
  *
  * Remove Duplicates based on date/merchant/state & city
  * Converts ISO Week into date
  * Parses the data from different files (only uses zip-merchant for now)
  * Joins with postal_code to obtain true City from Postal code
  */
object RawRefinedSegmintJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))
        val feedDate = cliArgs.date.toString

        //Source configuration, contract driven
        val srcDbName = contract.prop[String](getSchemaRawPath("db_name"))
        val srcTable = contract.prop[String](getSchemaRawPath("data_table"))

        //Supplement configuration, contract driven
        val postalCodeDb = contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val postalCodeTable = contract.prop[String](getSchemaRefinedHubPath("postal_table"))

        //Target configuration, contract driven
        val refinedSegmingDb = contract.prop[String](getSchemaRefinedSegmintPath("db_name"))
        val segmintTable = contract.prop[String](getSchemaRefinedSegmintPath("segmint_table"))

        val segmintRawDf = readSegmint(spark, feedDate, s"${srcDbName}.${srcTable}")
        val postalCodesDf = readPostalCodes(spark,s"$postalCodeDb.$postalCodeTable")

        val segmintTransformDf = transformSegmint(segmintRawDf, broadcast(postalCodesDf))

        writeSegmint(segmintTransformDf, s"$refinedSegmingDb.$segmintTable")
    }

    def readSegmint(spark: SparkSession, date: String, table: String): DataFrame = {
        import spark.implicits._

        val postalMerch = new StructType()
          .add("date", StringType)
          .add("merchant", StringType)
          .add("category", StringType)
          .add("location_type", StringType)
          .add("state", StringType)
          .add("postal_code", StringType)
          .add("distinct_customers", IntegerType)
          .add("transaction_quantity", IntegerType)
          .add("transaction_amount", DoubleType)
          .add("transaction_amount_avg", DoubleType)
        val iso8601DateConverterUdf = udf((date: String) => {
            val startingPossition = 0
            val lengthOfYear = "YYYY".length()
            val totalLengthDateFormat = "YYYY-WW".length()

            val year = date.substring(startingPossition, lengthOfYear)
            val week = date.substring(lengthOfYear + 1, totalLengthDateFormat)
            YearWeek.of(Integer.parseInt(year), Integer.parseInt(week)).atDay(DayOfWeek.MONDAY).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
        })
        spark.udf.register("iso8601DateConverter", iso8601DateConverterUdf)

        spark.table(table)
            .filter($"record_type" === "zip_merch" && $"feed_date" === date)
            .select(from_csv($"record_value", postalMerch, Map("sep" -> "|")).as("postal_merch"))
            .select(
              iso8601DateConverterUdf($"postal_merch.date").as("date"),
              $"postal_merch.merchant",
              $"postal_merch.location_type",
              $"postal_merch.state",
              $"postal_merch.postal_code",
              $"postal_merch.transaction_quantity",
              $"postal_merch.transaction_amount")
            .filter($"state" =!= "" && $"postal_code"  =!= "" && $"location_type".isNotNull)
    }

    def readPostalCodes(spark: SparkSession, table: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |    postal_code,
               |    city,
               |    region
               |FROM $table
               |""".stripMargin)
    }

    def transformSegmint(segmintRaw: DataFrame, postalCodes: DataFrame): DataFrame = {
        segmintRaw
            .join(postalCodes, segmintRaw("postal_code") === postalCodes("postal_code"), "left") // adds city, region
            .drop(postalCodes("postal_code"))
            .dropDuplicates("date", "merchant", "postal_code")
            .filter(col("city").isNotNull)
            .withColumn("region", coalesce(col("region"), lit("Unknown")))
            .withColumn("city", initcap(col("city")))
            .withColumn("location_type", upper(col("location_type")))
    }

    def writeSegmint(df: DataFrame, destTable: String): Unit = {
        val srcTable = "newSegmint"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.date = $srcTable.date AND $destTable.merchant = $srcTable.merchant AND $destTable.postal_code = $srcTable.postal_code
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    case class CliArgs(contractPath: String,
                       date: LocalDate,
                       sourceDateDirFormat: String = "yyyyMMdd") {
        val sourceDateDirFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(sourceDateDirFormat)
    }

    object CliArgs {
        @throws(classOf[IllegalArgumentException])
        def parse(args: Seq[String]): CliArgs = {
            args match {
                case Seq(contractPath, rawDate, sourceDateDirFormat) =>
                    CliArgs(contractPath, parseDate(rawDate), sourceDateDirFormat)
                case Seq(contractPath, rawDate) => CliArgs(contractPath, parseDate(rawDate))
                case _ => throw new IllegalArgumentException("Expected CLI arguments: <contractPath> <date (yyyy-MM-dd)> <sourceDateDirFormat?>")
            }
        }

        @throws(classOf[IllegalArgumentException])
        private def parseDate(rawDate: String): LocalDate = {
            Try(LocalDate.parse(rawDate, DateTimeFormatter.ISO_DATE)) match {
                case Success(date) => date
                case Failure(e) => throw new IllegalArgumentException(s"Unable to parse date from $rawDate, expected format is yyyy-MM-dd", e)
            }
        }
    }

    def getSchemaRawPath(relativePath: String): String = s"schema.raw.$relativePath"
    def getSchemaRefinedHubPath(relativePath: String): String = s"schema.refined_hub.$relativePath"
    def getSchemaRefinedSegmintPath(relativePath: String): String = s"schema.refined_segmint.$relativePath"
}
