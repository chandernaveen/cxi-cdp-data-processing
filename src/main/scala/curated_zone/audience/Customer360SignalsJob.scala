package com.cxi.cdp.data_processing
package curated_zone.audience

import curated_zone.audience.model.SignalUniverse
import support.SparkSessionFactory
import support.utils.ContractUtils
import support.utils.elasticsearch.ElasticsearchConfigUtils
import support.utils.elasticsearch.ElasticsearchConfigUtils.ElasticsearchConfig

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
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
        val esConfig = ElasticsearchConfigUtils.getElasticsearchConfig(spark, contract, esIndex)

        val curatedDb = contract.prop[String]("datalake.curated.db_name")
        val signalUniverseTableName = contract.prop[String]("datalake.curated.signal_universe_table")
        val customer360TableName = contract.prop[String]("datalake.curated.customer_360_table")
        val customer360SignalsTableName = contract.prop[String]("datalake.curated.customer_360_signals_table")

        val datalakeTablesConfig = DatalakeTablesConfig(
            signalUniverseTable = s"$curatedDb.$signalUniverseTableName",
            customer360Table = s"$curatedDb.$customer360TableName",
            customer360SignalsTable = s"$curatedDb.$customer360SignalsTableName")

        val feedDate = args(2)
        run(spark, esConfig, datalakeTablesConfig, feedDate)
    }


    def run(spark: SparkSession, esConfig: ElasticsearchConfig, dlConfig: DatalakeTablesConfig, feedDate: String): Unit = {
        val customer360signalsByDomain: DataFrame = process(spark, dlConfig, feedDate)
        saveToEs(esConfig, customer360signalsByDomain)
    }

    def process(spark: SparkSession, dlConfig: DatalakeTablesConfig, feedDate: String): DataFrame = {
        val (customer360Df, customer360SignalsDf, signalUniverseDs) = read(spark, dlConfig, feedDate)
        transform(customer360Df, customer360SignalsDf, signalUniverseDs)
    }

    def read(spark: SparkSession, dlConfig: DatalakeTablesConfig, feedDate: String): (DataFrame, DataFrame, Dataset[SignalUniverse]) = {
        import spark.implicits._
        val customer360Df = spark.table(dlConfig.customer360Table)
            .where(col("active_flag") === true)
            .select("customer_360_id", "identities")
        val signalsDf = spark.table(dlConfig.customer360SignalsTable)
            .where(col("signal_generation_date") === feedDate)
        val signalUniverseDs = spark.table(dlConfig.signalUniverseTable)
            .where(col("is_active") === true)
            .as[SignalUniverse]
        (customer360Df, signalsDf, broadcast(signalUniverseDs))
    }

    def transform(customer360df: DataFrame, signalsDf: DataFrame, signalUniverseDs: Dataset[SignalUniverse]): DataFrame = {

        // take only signals that exist in signal universe
        val recognizedSignals = signalsDf
            .withColumnRenamed("signal_domain", "domain_name")
            .join(signalUniverseDs, Seq("domain_name", "signal_name"), "inner")

        val customer360SignalsDf = customer360df
            .join(recognizedSignals, Seq("customer_360_id"), "left_outer")
            .withColumn("signal_es_name", concat(col("signal_name"), lit("_"), col("es_type")))
            .select("domain_name", "customer_360_id", "identities", "signal_es_name", "signal_value")

        val signalUniverse = signalUniverseDs.collect()

        val signalsByDomain = extractSignalDomainAsColumn(customer360SignalsDf)

        val signalsByDomainToMap = normalizeSignals(signalsByDomain, signalUniverse)

        val customer360signalsByDomain = signalsByDomainToMap.join(customer360df, "customer_360_id")
            .withColumnRenamed("identities", "customer_identities")
        customer360signalsByDomain
    }

    /**
      * Extracts all possible values from 'domain_name' column and generates columns with corresponding names, grouping by customer360
      */
    def extractSignalDomainAsColumn(signalsWithCustomerDetails: DataFrame): DataFrame = {
        signalsWithCustomerDetails.groupBy("customer_360_id")
            .pivot("domain_name")
            .agg(collect_list(struct(col("signal_es_name"), col("signal_value"))))
            // 'null' column is generated as there are customers that don't have any signals assigned
            .drop("null")
    }

    /**
      * Converts signal name-value pairs from array representation to map representation,
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
                        acc.withColumn(signal.domain_name,
                            when(size(col(signal.domain_name)) === 0, null).otherwise(map_from_entries(col(signal.domain_name))))
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

    case class DatalakeTablesConfig(signalUniverseTable: String, customer360Table: String, customer360SignalsTable: String)

}
