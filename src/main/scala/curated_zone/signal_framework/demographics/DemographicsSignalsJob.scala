package com.cxi.cdp.data_processing
package curated_zone.signal_framework.demographics

import refined_zone.hub.identity.model.IdentityType
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import java.nio.file.Paths
import scala.collection.immutable.{ListSet, Map}

object DemographicsSignalsJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    type SignalDomain = String
    type SignalName = String

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val spark = SparkSessionFactory.getSparkSession()

        val contractPath = "/mnt/" + args(0)
        val feedDate = args(1)

        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val curatedDb = contract.prop[String]("datalake.curated.db_name")
        val customer360TableName = contract.prop[String]("datalake.curated.customer_360_table")

        val refinedThrotleDb = contract.prop[String]("datalake.refined_throtle.db_name")
        val refinedThrotleTidAttTableName = contract.prop[String]("datalake.refined_throtle.tid_att_table")

        val customer360GenericDailySignalsTable =
            contract.prop[String]("datalake.curated.customer_360_generic_daily_signals_table")

        val customer360Df = readCustomer360WithThrotleIds(spark, s"$curatedDb.$customer360TableName")
        val signalNameToSignalDomain = getSignalNameToSignalDomainMapping
        val throtleTidAtt = readThrotleTidAttAttributesAsSignals(
            spark,
            s"$refinedThrotleDb.$refinedThrotleTidAttTableName",
            signalNameToSignalDomain.keys.to[ListSet]
        )

        val transformedDemographicsSignals = transform(customer360Df, throtleTidAtt, signalNameToSignalDomain, feedDate)

        transformedDemographicsSignals.foreach(tuple => {
            val (signalDomain, signalName, df) = tuple
            write(df, feedDate, signalDomain, signalName, s"$curatedDb.$customer360GenericDailySignalsTable")
        })
    }

    def transform(
        customer360Df: DataFrame,
        refinedThrotleTidAttDf: DataFrame,
        signalNameToSignalDomain: Map[SignalName, SignalDomain],
        feedDate: String
    ): Seq[(SignalDomain, SignalName, DataFrame)] = {
        val notSignalColumns = Set("customer_360_id", "throtle_id")

        val customer360WithDemographics = customer360Df
            .join(refinedThrotleTidAttDf, "throtle_id")
            .dropDuplicates("customer_360_id", "throtle_id")
            .drop("throtle_id")

        customer360WithDemographics.columns
            .filter(c => !notSignalColumns.contains(c))
            .map(signalName => {
                val signalDomain = signalNameToSignalDomain(signalName)
                (
                    signalDomain,
                    signalName,
                    customer360WithDemographics
                        .select("customer_360_id", signalName)
                        .filter(col(signalName).isNotNull)
                        .withColumn("signal_name", lit(signalName))
                        .groupBy("customer_360_id", "signal_name")
                        .agg(
                            element_at(
                                sort_array( // deterministically pick a single value
                                    collect_set(col(signalName).cast(StringType))
                                ),
                                1
                            ).as("signal_value")
                        )
                        .withColumn("signal_generation_date", lit(feedDate))
                        .withColumn("signal_domain", lit(signalDomain))
                        .select(
                            "customer_360_id",
                            "signal_generation_date",
                            "signal_domain",
                            "signal_name",
                            "signal_value"
                        )
                )
            })
    }

    def getSignalNameToSignalDomainMapping: Map[SignalName, SignalDomain] = {
        val profileSignalDomainName = "profile"
        val signalNameToSignalDomain = Map(
            "gender" -> profileSignalDomainName,
            "income" -> profileSignalDomainName,
            "age_range" -> profileSignalDomainName,
            "occupation" -> profileSignalDomainName,
            "children" -> profileSignalDomainName,
            "new_credit_range" -> profileSignalDomainName,
            "credit_ranges" -> profileSignalDomainName
        )
        signalNameToSignalDomain
    }

    def readCustomer360WithThrotleIds(spark: SparkSession, customer360Table: String): DataFrame = {
        spark
            .table(customer360Table)
            .select("customer_360_id", "identities")
            .where(col("active_flag") === true)
            .withColumn("throtle_ids", element_at(col("identities"), IdentityType.ThrotleId.code))
            .withColumn("throtle_id", explode(col("throtle_ids")))
            .select("customer_360_id", "throtle_id")
    }

    def readThrotleTidAttAttributesAsSignals(
        spark: SparkSession,
        refinedThrotleTidAttTableName: String,
        signalNames: ListSet[String]
    ): DataFrame = {
        val columnsToSelect: List[String] = List("throtle_id") ++ signalNames
        spark
            .table(refinedThrotleTidAttTableName)
            .select(columnsToSelect.map(col): _*)
    }

    def write(
        df: DataFrame,
        feedDate: String,
        signalDomain: SignalDomain,
        signalName: SignalName,
        destTable: String
    ): Unit = {
        val srcTable = s"new_customer_360_generic_daily_signals_${signalDomain}_${signalName}"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |INSERT OVERWRITE TABLE $destTable
               |PARTITION(signal_generation_date = '$feedDate', signal_domain = '$signalDomain', signal_name = '$signalName')
               |SELECT customer_360_id, signal_value FROM $srcTable
               |""".stripMargin)
    }

}
