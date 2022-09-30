package com.cxi.cdp.data_processing
package curated_zone.audience

import com.cxi.cdp.data_processing.curated_zone.model.CustomerMetricsTimePeriod
import curated_zone.audience.model.{SignalType, SignalUniverse}
import support.utils.elasticsearch.ElasticsearchConfigUtils
import support.utils.elasticsearch.ElasticsearchConfigUtils.ElasticsearchConfig
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.elasticsearch.spark.sql.sparkDatasetFunctions

import java.nio.file.Paths

object Customer360SignalsJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        val contractPath = "/mnt/" + args(0)
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val esIndex = args(1)
        val esConfig = ElasticsearchConfigUtils.getElasticsearchConfig(spark, contract, esIndex, "customer_360_id")

        val curatedDb = contract.prop[String]("datalake.curated.db_name")
        val signalUniverseTableName = contract.prop[String]("datalake.curated.signal_universe_table")
        val customer360TableName = contract.prop[String]("datalake.curated.customer_360_table")
        val customer360GenericDailySignalsTableName =
            contract.prop[String]("datalake.curated.customer_360_generic_daily_signals_table")
        val partnerLocationWeeklySignalsTableName =
            contract.prop[String]("datalake.curated.partner_location_weekly_signals_table")
        val partnerLocationDailySignalsTableName =
            contract.prop[String]("datalake.curated.partner_location_daily_signals_table")
        val refinedHubDbName = contract.prop[String]("datalake.refined_hub.db_name")
        val locationTableName = contract.prop[String]("datalake.refined_hub.location_table")

        val datalakeTablesConfig = DatalakeTablesConfig(
            signalUniverseTable = s"$curatedDb.$signalUniverseTableName",
            customer360Table = s"$curatedDb.$customer360TableName",
            customer360GenericDailySignalsTable = s"$curatedDb.$customer360GenericDailySignalsTableName",
            partnerLocationWeeklySignalsTable = s"$curatedDb.$partnerLocationWeeklySignalsTableName",
            partnerLocationDailySignalsTable = s"$curatedDb.$partnerLocationDailySignalsTableName",
            locationTable = s"$refinedHubDbName.$locationTableName"
        )

        val feedDate = args(2)
        run(spark, esConfig, datalakeTablesConfig, feedDate)
    }

    def run(
        spark: SparkSession,
        esConfig: ElasticsearchConfig,
        dlConfig: DatalakeTablesConfig,
        feedDate: String
    ): Unit = {
        val customer360signalsByDomain: DataFrame = process(spark, dlConfig, feedDate)
        saveToEs(esConfig, customer360signalsByDomain)
    }

    def process(spark: SparkSession, dlConfig: DatalakeTablesConfig, feedDate: String): DataFrame = {
        val customer360Df = readCustomer360(spark, dlConfig.customer360Table)
        val signalUniverseDs = readSignalUniverse(spark, dlConfig.signalUniverseTable)

        val genericSignalsTransformed = processGenericCustomerSignals(
            spark,
            dlConfig.customer360GenericDailySignalsTable,
            signalUniverseDs,
            feedDate
        )
        val partnerLocationSignalsTransformed = processPartnerLocationSignals(
            spark,
            dlConfig.locationTable,
            dlConfig.partnerLocationWeeklySignalsTable,
            dlConfig.partnerLocationDailySignalsTable,
            signalUniverseDs,
            feedDate
        )

        customer360Df
            .join(genericSignalsTransformed, Seq("customer_360_id"), "left_outer")
            .join(partnerLocationSignalsTransformed, Seq("customer_360_id"), "left_outer")
            .withColumnRenamed("identities", "customer_identities")
    }

    def readCustomer360(spark: SparkSession, customer360TableName: String): DataFrame = {
        spark
            .table(customer360TableName)
            .where(col("active_flag") === true)
            .select("customer_360_id", "identities")
    }

    def readSignalUniverse(spark: SparkSession, signalUniverseTableName: String): Dataset[SignalUniverse] = {
        import spark.implicits._
        broadcast(
            spark
                .table(signalUniverseTableName)
                .where(col("is_active") === true)
                .as[SignalUniverse]
        )
    }

    def processGenericCustomerSignals(
        spark: SparkSession,
        genericDailyCustomerSignalsTableName: String,
        signalUniverseDs: Dataset[SignalUniverse],
        feedDate: String
    ): DataFrame = {
        val customer360SignalsDf: DataFrame =
            readGenericDailyCustomerSignals(spark, genericDailyCustomerSignalsTableName, feedDate)
        val genericCustomerSignalsConfig =
            signalUniverseDs.filter(_.signal_type == SignalType.GeneralCustomerSignal.code)
        val genericSignalsTransformed = transformGenericSignals(customer360SignalsDf, genericCustomerSignalsConfig)
        genericSignalsTransformed
    }

    def readGenericDailyCustomerSignals(spark: SparkSession, tableName: String, feedDate: String): DataFrame = {
        spark
            .table(tableName)
            .where(col("signal_generation_date") === feedDate)
    }

    def processPartnerLocationSignals(
        spark: SparkSession,
        locationTableName: String,
        partnerLocationWeeklySignalsTableName: String,
        partnerLocationDailySignalsTable: String,
        signalUniverseDs: Dataset[SignalUniverse],
        feedDate: String
    ): DataFrame = {
        val specificPartnerLocationSignalsConfig =
            signalUniverseDs.filter(_.signal_type == SignalType.SpecificPartnerLocationSignal.code)

        val locationsDf = transformLocations(spark, readLocations(spark, locationTableName))
        val partnerLocationWeeklySignalsDf =
            readPartnerLocationWeeklySignals(spark, partnerLocationWeeklySignalsTableName, feedDate)
        val partnerLocationDailySignalsDf =
            readPartnerLocationDailySignals(spark, partnerLocationDailySignalsTable, feedDate)

        transformPartnerLocationSignals(
            spark,
            partnerLocationWeeklySignalsDf.unionByName(partnerLocationDailySignalsDf),
            specificPartnerLocationSignalsConfig,
            locationsDf
        )
    }

    def readLocations(spark: SparkSession, locationTableName: String): DataFrame = {
        broadcast(spark.table(locationTableName).select("cxi_partner_id", "location_id", "location_nm"))
    }

    def transformLocations(spark: SparkSession, locations: DataFrame): DataFrame = {
        import spark.implicits._
        locations.withColumn("location_nm", coalesce($"location_nm", $"location_id"))
    }

    def readPartnerLocationWeeklySignals(
        spark: SparkSession,
        partnerLocationWeeklySignalsTableName: String,
        feedDate: String
    ): DataFrame = {
        val feedDateCol = to_date(lit(feedDate), "yyyy-MM-dd")
        val daysInAWeek = 7
        val excludeCurrentDay = 0

        val partnerLocationWeeklyTableWithLastWeekData = spark
            .table(partnerLocationWeeklySignalsTableName)
            .where(
                col("signal_generation_date") between (date_sub(feedDateCol, daysInAWeek), date_sub(
                    feedDateCol,
                    excludeCurrentDay
                ))
            )

        val latestGeneratedSignals = partnerLocationWeeklyTableWithLastWeekData
            .select("signal_generation_date", "signal_domain", "signal_name")
            .distinct()
            .groupBy("signal_domain", "signal_name")
            .agg(max("signal_generation_date").as("signal_generation_date"))

        val partnerLocationWeeklySignalsDf = partnerLocationWeeklyTableWithLastWeekData
            .join(
                broadcast(latestGeneratedSignals),
                Seq("signal_generation_date", "signal_domain", "signal_name"),
                "inner"
            )
            .select(
                "cxi_partner_id",
                "location_id",
                "date_option",
                "customer_360_id",
                "signal_domain",
                "signal_name",
                "signal_value"
            )
        partnerLocationWeeklySignalsDf
    }

    def readPartnerLocationDailySignals(
        spark: SparkSession,
        partnerLocationDailySignalsTableName: String,
        feedDate: String
    ): DataFrame = {
        val feedDateCol = to_date(lit(feedDate), "yyyy-MM-dd")

        spark
            .table(partnerLocationDailySignalsTableName)
            .select(
                "cxi_partner_id",
                "location_id",
                "date_option",
                "customer_360_id",
                "signal_domain",
                "signal_name",
                "signal_value"
            )
            .where(col("signal_generation_date") === feedDateCol)
    }

    def transformPartnerLocationSignals(
        spark: SparkSession,
        partnerLocationSignalsDf: DataFrame,
        signalUniverseDs: Dataset[SignalUniverse],
        locationsDf: DataFrame
    ): DataFrame = {
        import spark.implicits._

        val recognizedSignals = getPartnerLocationRecognizedSignals(partnerLocationSignalsDf, signalUniverseDs)

        val signalUniverse = signalUniverseDs.collect()
        val partnerLocationSignalsByDomain = extractPartnerLocationSignalDomainAsColumn(recognizedSignals)
        val signalsByDomainToMap = normalizeSignals(partnerLocationSignalsByDomain, signalUniverse)

        val partnerLocationSignalsGroupedByDomainsAndTimePeriods = groupPartnerLocationSignalsByDomainAndTimePeriod(
            signalsByDomainToMap
        )

        val partnerLocationSignalsEnrichedWithLocationNames = partnerLocationSignalsGroupedByDomainsAndTimePeriods
            .join(locationsDf, Seq("cxi_partner_id", "location_id"), "left") // enrich with location name
            .withColumn("location_nm", coalesce($"location_nm", $"location_id"))
            .select(
                "customer_360_id",
                "es_field_parent_section_name",
                "cxi_partner_id",
                "location_id",
                "location_nm",
                "time_periods"
            )

        val partnerLocationEntryColumn: List[Column] =
            List(
                $"cxi_partner_id".as("cxi_partner_id_keyword"),
                $"location_id".as("location_id_keyword"),
                $"location_nm".as("location_name_keyword")
            ) ++ CustomerMetricsTimePeriod.values.map(tp => s"time_periods.${tp.value}").map(col)

        val customerPartnerLocationData = partnerLocationSignalsEnrichedWithLocationNames
            .withColumn("partner_location_entry", struct(partnerLocationEntryColumn: _*))
            .select("customer_360_id", "es_field_parent_section_name", "partner_location_entry")

        customerPartnerLocationData
            .groupBy("customer_360_id")
            .pivot("es_field_parent_section_name")
            .agg(collect_list("partner_location_entry"))
    }

    private def getPartnerLocationRecognizedSignals(
        partnerLocationSignalsDf: DataFrame,
        signalUniverseDs: Dataset[SignalUniverse]
    ): DataFrame = {
        // take only signals that exist in signal universe
        partnerLocationSignalsDf
            .withColumnRenamed("signal_domain", "domain_name")
            .join(signalUniverseDs, Seq("domain_name", "signal_name"), "inner")
            .withColumn("signal_es_name", concat(col("signal_name"), lit("_"), col("es_type")))
            .select(
                "domain_name",
                "signal_es_name",
                "es_field_parent_section_name",
                "customer_360_id",
                "cxi_partner_id",
                "location_id",
                "date_option",
                "signal_value"
            )
    }

    def groupPartnerLocationSignalsByDomainAndTimePeriod(signalsByDomainToMap: DataFrame): DataFrame = {
        val notDomainColumns =
            List("customer_360_id", "es_field_parent_section_name", "cxi_partner_id", "location_id", "date_option")
        val keyValueDomainColumnsMapping: Array[Column] =
            signalsByDomainToMap.columns
                .filter(c => !notDomainColumns.contains(c))
                .flatMap { c => Array(lit(c), col(c)) }

        val partnerLocationSignalsGroupedByDomainsAndTimePeriods = signalsByDomainToMap
            .withColumn("domains_as_map", map(keyValueDomainColumnsMapping: _*))
            .withColumn("domains_as_map_filtered", map_filter(col("domains_as_map"), (k, v) => v.isNotNull))
            .withColumn("time_period", struct(col("date_option"), col("domains_as_map_filtered")))
            .select("time_period", "customer_360_id", "es_field_parent_section_name", "cxi_partner_id", "location_id")
            .groupBy("customer_360_id", "es_field_parent_section_name", "cxi_partner_id", "location_id")
            .agg(collect_list(col("time_period")).as("time_periods"))
            .withColumn("time_periods", map_from_entries(col("time_periods")))
        partnerLocationSignalsGroupedByDomainsAndTimePeriods
    }

    def transformGenericSignals(signalsDf: DataFrame, signalUniverseDs: Dataset[SignalUniverse]): DataFrame = {

        // take only signals that exist in signal universe
        val recognizedSignals = signalsDf
            .withColumnRenamed("signal_domain", "domain_name")
            .join(signalUniverseDs, Seq("domain_name", "signal_name"), "inner")

        val customer360SignalsDf = recognizedSignals
            .withColumn("signal_es_name", concat(col("signal_name"), lit("_"), col("es_type")))
            .select("es_field_parent_section_name", "domain_name", "customer_360_id", "signal_es_name", "signal_value")

        val signalUniverse = signalUniverseDs.collect()

        val signalsByDomain = extractSignalDomainAsColumn(customer360SignalsDf)

        val signalsByDomainToMap = normalizeSignals(signalsByDomain, signalUniverse)

        val groupingColumns = Set("customer_360_id", "es_field_parent_section_name")
        val keyValueDomainColumnsMapping: Array[Column] =
            signalsByDomainToMap.columns.filter(c => !groupingColumns.contains(c)).flatMap { c =>
                Array(lit(c), col(c))
            }

        val esParentSectionColNames = signalUniverse.map(_.es_field_parent_section_name).toSet
        val uniqueEsSectionNames: Set[Column] =
            esParentSectionColNames.map(section => col("generic_customer_signals")(section).as(section))
        val allResultingColumns: Seq[Column] = col("customer_360_id") +: uniqueEsSectionNames.toList
        signalsByDomainToMap
            .withColumn("domains_as_map", map(keyValueDomainColumnsMapping: _*))
            .withColumn("domains_as_map_filtered", map_filter(col("domains_as_map"), (k, v) => v.isNotNull))
            .withColumn(
                "generic_customer_signals_struct",
                struct(col("es_field_parent_section_name"), col("domains_as_map_filtered"))
            )
            .groupBy("customer_360_id")
            .agg(
                collect_list("generic_customer_signals_struct").as("generic_customer_signals_structs")
            )
            .withColumn("generic_customer_signals", map_from_entries(col("generic_customer_signals_structs")))
            .select(allResultingColumns: _*)
    }

    def extractPartnerLocationSignalDomainAsColumn(signalsWithCustomerDetails: DataFrame): DataFrame = {
        signalsWithCustomerDetails
            .groupBy("customer_360_id", "es_field_parent_section_name", "cxi_partner_id", "location_id", "date_option")
            .pivot("domain_name")
            .agg(collect_list(struct(col("signal_es_name"), col("signal_value"))))
    }

    /** Extracts all possible values from 'domain_name' column and generates columns with corresponding names, grouping by customer360
      */
    def extractSignalDomainAsColumn(signalsWithCustomerDetails: DataFrame): DataFrame = {
        signalsWithCustomerDetails
            .groupBy("customer_360_id", "es_field_parent_section_name")
            .pivot("domain_name")
            .agg(collect_list(struct(col("signal_es_name"), col("signal_value"))))
            // 'null' column is generated as there are customers that don't have any signals assigned
            .drop("null")
    }

    /** Converts signal name-value pairs from array representation to map representation,
      * e.g. [{delivery_preferred_boolean, true}] -> {delivery_preferred_boolean -> true}, empty arrays should be transformed to null: [] -> null
      *
      * @param signalsByDomain source DF with separate column per each signal domain, and signal name-value pairs (as array) within each 'signal domain' column
      * @param signalUniverse array of signals configuration, we need it to detect 'signal domain' columns in the source DF
      */
    def normalizeSignals(signalsByDomain: DataFrame, signalUniverse: Array[SignalUniverse]): DataFrame = {
        signalUniverse.foldLeft(signalsByDomain)((acc, signal) => {
            if (acc.schema.fieldNames.contains(signal.domain_name)) {
                acc.schema(signal.domain_name).dataType match {
                    case ArrayType(_, _) =>
                        acc.withColumn(
                            signal.domain_name,
                            when(size(col(signal.domain_name)) === 0, null)
                                .otherwise(map_from_entries(col(signal.domain_name)))
                        )
                    case _ => acc
                }
            } else {
                acc
            }
        })
    }

    private def saveToEs(esConfig: ElasticsearchConfig, customer360signalsByDomain: DataFrame): Unit = {
        customer360signalsByDomain
            .saveToEs(esConfig.esIndex, esConfig.esConfigMap)
    }

    case class DatalakeTablesConfig(
        signalUniverseTable: String,
        customer360Table: String,
        customer360GenericDailySignalsTable: String,
        partnerLocationWeeklySignalsTable: String,
        partnerLocationDailySignalsTable: String,
        locationTable: String
    )

}
