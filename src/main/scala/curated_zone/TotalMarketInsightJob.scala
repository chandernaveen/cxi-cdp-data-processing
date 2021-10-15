package com.cxi.cdp.data_processing
package curated_zone

import support.SparkSessionFactory
import support.packages.utils.ContractUtils

import com.databricks.service.DBUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

object TotalMarketInsightJob {
    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val contractPath = args(0)
        val date = args(1)
        val spark = SparkSessionFactory.getSparkSession()
        run(spark, contractPath, date)
    }

    def run(spark: SparkSession, contractPath: String, date: String): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val refinedSrcDbName = contract.prop[String]("refined.db")
        val orderSummaryTable = contract.prop[String]("refined.order_summary.table")
        val refinedHubSrcTable = contract.prop[String]("refined_hub.db")
        val partnerTable = contract.prop[String]("refined_hub.partner.table")
        val destDbName = contract.prop[String]("curated.db")
        val destPartnerMarketInsightsTableName = contract.prop[String]("curated.partner_market_insights.table")
        val destTotalMarketInsightsTableName = contract.prop[String]("curated.total_market_insights.table")

        val orderSummary: DataFrame = readOrderSummary(spark, date, refinedSrcDbName, orderSummaryTable)
        val partners: DataFrame = readPartners(spark, refinedHubSrcTable, partnerTable)

        val orderSummaryWithPartnerInfo = addPartnerInfoToOrderSummary(orderSummary, partners)

        val partnerMarketInsights = computePartnerMarketInsights(orderSummaryWithPartnerInfo)
            .cache()
        val totalMarketInsightPerPartnerType = computeTotalMarketInsights(partnerMarketInsights)
            .cache()

        writeToDatalakePartnerMarketInsights(partnerMarketInsights, contract, destDbName, destPartnerMarketInsightsTableName)
        writeToMongoPartnerMarketInsights(partnerMarketInsights, contract)

        writeToDatalakeTotalMarketInsights(totalMarketInsightPerPartnerType, contract, destDbName, destTotalMarketInsightsTableName)
        writeToMongoTotalMarketInsights(totalMarketInsightPerPartnerType, contract)
    }

    def readOrderSummary(spark: SparkSession, date: String, refinedSrcDbName: String, orderSummaryTable: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT
               |cxi_partner_id,
               |ord_date,
               |location_id,
               |item_total,
               |ord_id
               |FROM $refinedSrcDbName.$orderSummaryTable
               |WHERE ord_state = "true" AND ord_date = "$date"
               |""".stripMargin)
    }

    def readPartners(spark: SparkSession, refinedHubSrcTable: String, partnerTable: String): DataFrame = {
        spark.sql(
            s"""
               |SELECT cxi_partner_id, partner_type
               |FROM $refinedHubSrcTable.$partnerTable
               |""".stripMargin)
    }

    def addPartnerInfoToOrderSummary(orderSummary: DataFrame, partners: DataFrame): DataFrame = {
        orderSummary
            .join(broadcast(partners), Seq("cxi_partner_id"), "left_outer")
            .select("partner_type", orderSummary.schema.fieldNames: _*)
    }

    def computeTotalMarketInsights(partnerMarketInsights: DataFrame): DataFrame = {
        partnerMarketInsights
            .groupBy("partner_type", "ord_date")
            .agg(
                countDistinct("location_id").as("partner_type_locations_count"),
                sum("amount_of_sales").as("partner_type_total_sales"),
                sum("foot_traffic").as("foot_traffic")
            )
            .withColumn("partner_type_sales_per_location", col("partner_type_total_sales") / col("partner_type_locations_count"))
            .withColumn("location_city", lit(null))
    }

    def computePartnerMarketInsights(orderSummaryWithPartnerInfo: DataFrame): DataFrame = {
        orderSummaryWithPartnerInfo
            .groupBy("cxi_partner_id", "location_id", "ord_date", "partner_type")
            .agg(
                sum("item_total") as "amount_of_sales",
                countDistinct("ord_id") as "foot_traffic"
            )
    }

    def writeToDatalakePartnerMarketInsights(partnerMarketInsights: DataFrame, contract: ContractUtils, destDbName: String, destTMITableName: String): Unit = {
        val srcTable = "newPartnerMarketInsight"

        val partnerMarketInsightsRenamed = partnerMarketInsights
            .withColumnRenamed("ord_date", "date")
            .select("cxi_partner_id", "partner_type", "date", "location_id", "amount_of_sales", "foot_traffic")

        partnerMarketInsightsRenamed.createOrReplaceTempView(srcTable)
        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        partnerMarketInsightsRenamed.sqlContext.sql(
            s"""
               |MERGE INTO $destDbName.$destTMITableName
               |USING $srcTable
               |ON $destDbName.$destTMITableName.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destDbName.$destTMITableName.date <=> $srcTable.date
               | AND $destDbName.$destTMITableName.location_id <=> $srcTable.location_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoPartnerMarketInsights(partnerMarketInsights: DataFrame, contract: ContractUtils): Unit = {
        val username = DBUtils.secrets.get(contract.prop[String]("mongo.secret_scope"), contract.prop[String]("mongo.username_secret_key"))
        val password = DBUtils.secrets.get(contract.prop[String]("mongo.secret_scope"), contract.prop[String]("mongo.password_secret_key"))
        val scheme = contract.prop[String]("mongo.scheme")
        val host = contract.prop[String]("mongo.host")

        partnerMarketInsights
            .withColumnRenamed("ord_date", "date")
            .select("cxi_partner_id", "partner_type", "date", "location_id", "amount_of_sales", "foot_traffic")
            .write
            .format("com.mongodb.spark.sql.DefaultSource")
            .mode("append")
            .option("database", contract.prop[String]("mongo.db"))
            .option("collection", contract.prop[String]("mongo.partner_market_insights_collection"))
            .option("uri", s"$scheme$username:$password@$host")
//             todo: add shard key for deduplication https://docs.mongodb.com/manual/core/sharding-choose-a-shard-key/, https://docs.mongodb.com/spark-connector/master/configuration/#output-configuration
//            .option("replaceDocument", "true")
//            .option("shardKey", """{"date": 1, "name": 1, "resource": 1}""") // example
            .save()
    }

    def writeToDatalakeTotalMarketInsights(totalMarketInsights: DataFrame, contract: ContractUtils, destDbName: String, destTMITotalTableName: String): Unit = {
        val srcTable = "newTotalMarketInsight"

        val totalMarketInsightsRenamed = totalMarketInsights
            .withColumnRenamed("ord_date", "date")
            .withColumnRenamed("partner_type_sales_per_location", "amount_of_sales")
            .select("partner_type", "date", "amount_of_sales", "location_city", "foot_traffic")

        totalMarketInsightsRenamed.createOrReplaceTempView(srcTable)
        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        totalMarketInsightsRenamed.sqlContext.sql(
            s"""
               |MERGE INTO $destDbName.$destTMITotalTableName
               |USING $srcTable
               |ON $destDbName.$destTMITotalTableName.partner_type <=> $srcTable.partner_type
               | AND $destDbName.$destTMITotalTableName.date <=> $srcTable.date
               | AND $destDbName.$destTMITotalTableName.location_city <=> $srcTable.location_city
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoTotalMarketInsights(totalMarketInsights: DataFrame, contract: ContractUtils): Unit = {
        val username = DBUtils.secrets.get(contract.prop[String]("mongo.secret_scope"), contract.prop[String]("mongo.username_secret_key"))
        val password = DBUtils.secrets.get(contract.prop[String]("mongo.secret_scope"), contract.prop[String]("mongo.password_secret_key"))
        val scheme = contract.prop[String]("mongo.scheme")
        val host = contract.prop[String]("mongo.host")

        totalMarketInsights
            .withColumnRenamed("ord_date", "date")
            .withColumnRenamed("partner_type_sales_per_location", "amount_of_sales")
            .select("partner_type", "date", "amount_of_sales", "location_city", "foot_traffic")
            .write
            .format("com.mongodb.spark.sql.DefaultSource")
            .mode("append")
            .option("database", contract.prop[String]("mongo.db"))
            .option("collection", contract.prop[String]("mongo.total_market_insights_collection"))
            .option("uri", s"$scheme$username:$password@$host")
//             todo: add shard key for deduplication https://docs.mongodb.com/manual/core/sharding-choose-a-shard-key/, https://docs.mongodb.com/spark-connector/master/configuration/#output-configuration
//            .option("replaceDocument", "true")
//            .option("shardKey", """{"date": 1, "name": 1, "resource": 1}""") // example
            .save()
    }
}
