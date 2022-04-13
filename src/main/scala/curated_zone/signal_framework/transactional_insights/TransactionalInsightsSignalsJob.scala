package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights

import curated_zone.model.CustomerMetricsTimePeriod
import curated_zone.model.signal.transactional_insights.{ChannelMetric, OrderMetric, TenderTypeMetric, TimeOfDayMetric}
import support.SparkSessionFactory
import support.utils.ContractUtils

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object TransactionalInsightsSignalsJob {

    private val logger = Logger.getLogger(this.getClass.getName)
    private final val DateFormat = "yyyy-MM-dd"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        val contractPath = "/mnt/" + args(0)
        val feedDate = args(1)

        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val curatedDb = contract.prop[String]("datalake.curated.db_name")
        val preAggTableName = contract.prop[String]("datalake.curated.pre_aggr_transactional_insights_table")
        val signalsTableName = contract.prop[String]("datalake.curated.customer_360_partner_location_daily_signals_table")

        val preAggTable = s"$curatedDb.$preAggTableName"
        val signalTable = s"$curatedDb.$signalsTableName"

        run(spark, feedDate, preAggTable, signalTable)
    }

    def run(spark: SparkSession, feedDate: String, preAggTable: String, signalsTable: String): Unit = {
        val signalsDf = process(spark, feedDate, preAggTable)
        write(signalsDf, signalsTable, feedDate)
    }

    def process(spark: SparkSession, feedDate: String, preAggTable: String): DataFrame = {
        val preAggDf = read(spark, preAggTable)

        val signals7Days = collectSignalsForTimePeriod(feedDate, CustomerMetricsTimePeriod.Period7days, preAggDf)
        val signals30Days = collectSignalsForTimePeriod(feedDate, CustomerMetricsTimePeriod.Period30days, preAggDf)
        val signals60Days = collectSignalsForTimePeriod(feedDate, CustomerMetricsTimePeriod.Period60days, preAggDf)
        val signals90Days = collectSignalsForTimePeriod(feedDate, CustomerMetricsTimePeriod.Period90days, preAggDf)

        signals7Days
            .unionByName(signals30Days)
            .unionByName(signals60Days)
            .unionByName(signals90Days)
    }

    private def collectSignalsForTimePeriod(feedDate: String, timePeriod: CustomerMetricsTimePeriod, preAggDf: DataFrame): DataFrame = {
        // need 1 day adjustment as current date is included as well
        val startDate = LocalDate.parse(feedDate).minusDays(timePeriod.numberOfDays - 1).format(DateTimeFormatter.ofPattern(DateFormat))

        preAggDf
            .where(col("ord_date").between(startDate, feedDate))
            .groupBy("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name")
            .agg(sum("signal_value") as "signal_value")
            .withColumn("date_option", lit(timePeriod.value))
    }

    private def read(spark: SparkSession, preAggTable: String): DataFrame = {
        spark.table(preAggTable)
    }

    private[transactional_insights] def write(df: DataFrame, destTable: String, feedDate: String): Unit = {

        val transactionalInsightsSignalDomains =
            Seq(ChannelMetric.signalDomainName, OrderMetric.signalDomainName, TenderTypeMetric.signalDomainName, TimeOfDayMetric.signalDomainName)

        val transactionalInsightsSignalNames =
            ChannelMetric.signalNames ++ OrderMetric.signalNames ++ TenderTypeMetric.signalNames ++ TimeOfDayMetric.signalNames

        val srcTable = "new_customer_360_partner_location_daily_signals"
        df.createOrReplaceTempView(srcTable)

        df.sqlContext.sql(
            s"""
               |DELETE FROM $destTable
               |    WHERE signal_generation_date = '$feedDate'
               |    AND signal_domain IN ${transactionalInsightsSignalDomains.mkString("('", "', '", "')")}
               |    AND signal_name IN ${transactionalInsightsSignalNames.mkString("('", "', '", "')")}
               |""".stripMargin)

        df.sqlContext.sql(
            s"""
               |INSERT INTO $destTable
               |PARTITION(signal_generation_date = '$feedDate', signal_domain, signal_name, cxi_partner_id)
               |(cxi_partner_id, location_id, date_option, customer_360_id, signal_domain, signal_name, signal_value)
               |SELECT cxi_partner_id, location_id, date_option, customer_360_id, signal_domain, signal_name, signal_value FROM $srcTable
               |""".stripMargin)
    }

}
