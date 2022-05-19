package com.cxi.cdp.data_processing
package refined_zone.segmint

import support.SparkSessionFactory
import support.normalization.DateNormalization
import support.utils.ContractUtils

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.temporal.TemporalAdjusters
import java.time.{DayOfWeek, LocalDate}
import scala.util.{Failure, Success, Try}

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

        val iso8601DateConverterUdf = udf(convertYearWeekToIso8601Date _)

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

    /**
      * Converts date (string) in 'YYYY-WW' format to the date (string) in 'yyyy-MM-dd' format.
      * Number of the week is converted to the exact date based on the fact, that Segmint ingestion happens on Saturdays.
      * (Friday is treated as the last day of the week).
      * @param yearWeekDate date in 'YYYY-WW' format
      * @return date in 'yyyy-MM-dd' format
      */
    def convertYearWeekToIso8601Date(yearWeekDate: String): String = {
        val startingPosition = 0
        val lengthOfYear = "YYYY".length()
        val totalLengthDateFormat = "YYYY-WW".length()

        val year = Integer.parseInt(yearWeekDate.substring(startingPosition, lengthOfYear))
        val week = Integer.parseInt(yearWeekDate.substring(lengthOfYear + 1, totalLengthDateFormat))

        val firstDayOfYear = LocalDate.of(year, 1, 1)
        val firstSaturdayOfYear = firstDayOfYear.`with`(TemporalAdjusters.firstInMonth(DayOfWeek.SATURDAY))
        val nextWeekFirstDay = firstSaturdayOfYear.plusWeeks(week)
        val lastDayOfCurrentWeek = nextWeekFirstDay.minusDays(1)
        DateNormalization.formatFromLocalDate(lastDayOfCurrentWeek).get
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
