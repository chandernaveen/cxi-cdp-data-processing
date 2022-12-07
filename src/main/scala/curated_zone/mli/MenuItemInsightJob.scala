package com.cxi.cdp.data_processing
package curated_zone.mli
import refined_zone.hub.model.{LocationType, OrderStateType}
import refined_zone.hub.ChangeDataFeedViews
import support.utils.mongodb.MongoDbConfigUtils
import support.utils.mongodb.MongoDbConfigUtils.MongoSparkConnectorClass
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.nio.file.Paths
object MenuItemInsightJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val consumer_id = "partner_item_insights_job"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        val dataServicesDb = contract.prop[String]("schema.data_services.db_name")
        val cdfTrackerTable = contract.prop[String]("schema.data_services.cdf_tracker_table")

        val orderSummaryTables = contract.prop[Seq[String]]("schema.order_summary_tables")

        val orderSummaryCdf = ChangeDataFeedViews.orderSummary(s"$dataServicesDb.$cdfTrackerTable", orderSummaryTables)

        val orderSummaryChangeDataResult =
            if (cliArgs.fullReprocess) {
                orderSummaryCdf.queryAllData(consumer_id)
            } else {
                orderSummaryCdf.queryChangeData(consumer_id)
            }
        orderSummaryChangeDataResult.data match {
            case None => logger.info("No updates found since the last run")

            case Some(changeData) =>
                val orderDates = getOrderDatesToProcess(changeData)
                if (orderDates.isEmpty) {
                    logger.info(s"No order dates to process")
                } else {
                    logger.info(s"Order dates to process: $orderDates")
                    process(contract, orderDates, cliArgs.fullReprocess)
                }

                logger.info(s"Update CDF tracker: ${orderSummaryChangeDataResult.tableMetadataSeq}")
                orderSummaryCdf.markProcessed(orderSummaryChangeDataResult)
        }
    }

    def process(contract: ContractUtils, orderDates: Set[String], fullReprocess: Boolean)(implicit
        spark: SparkSession
    ): Unit = {
        val refinedHubDb = contract.prop[String]("schema.refined_hub.db_name")
        val orderSummaryTable = contract.prop[String]("schema.refined_hub.order_summary_table")
        val locationTable = contract.prop[String]("schema.refined_hub.location_table")
        val itemTable = contract.prop[String]("schema.refined_hub.item_table")
        val curatedhubDB = contract.prop[String]("schema.curated_hub.db_name")
        val customer360table = contract.prop[String]("schema.curated_hub.customer_360_table")
        val itemUniversetable = contract.prop[String]("schema.curated_hub.item_universe_table")
        val curatedcxiInsightDB = contract.prop[String]("schema.curated_cxi_insights_db_name")
        val partnerItemInsightsTable = contract.prop[String]("schema.curated_cxi_insight.partner_item_insights_table")
        val partnerCategoryInsightsTable =
            contract.prop[String]("schema.curated_cxi_insight.partner_category_insights_table")
        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDbName = contract.prop[String]("mongo.db")
        val partnerItemInsightsMongoCollectionName = contract.prop[String]("mongo.partner_item_insights_collection")
        val partnerCategoryInsightsMongoCollectionName =
            contract.prop[String]("mongo.partner_category_insights_collection")
        val orderSummary: DataFrame =
            readOrderSummary(
                orderDates,
                s"$refinedHubDb.$orderSummaryTable",
                s"$refinedHubDb.$locationTable",
                s"$refinedHubDb.$itemTable")
        val customer360: DataFrame =
            readCustomer360(
                s"$curatedhubDB.$customer360table",
                s"$refinedHubDb.$orderSummaryTable",
                s"$curatedhubDB.itemuniversetable",
                s"$refinedHubDb.$orderSummaryTable")
        val item: DataFrame =
            readitem(s"$refinedHubDb.$itemTable")
        val partnerItemInsights: DataFrame = computePartnerItemInsights(orderSummary).cache()
        writeToDatalakePartnerItemInsights(
            partnerItemInsights,
            s"$curatedcxiInsightDB.$partnerItemInsightsTable",
            fullReprocess
        )
        writeToMongoPartnerItemInsights(
            partnerItemInsights,
            mongoDbConfig.uri,
            mongoDbName,
            partnerItemInsightsMongoCollectionName,
            fullReprocess
        )

    }

    private def shouldComputePartnerCategoryInsights(contract: ContractUtils): Boolean = {
        contract.propOrElse[Boolean](
            "jobs.databricks.total_Item_insights_job.job_config.compute_total_Item_insights",
            true
        )

    }

    def getOrderDatesToProcess(orderSummaryChangeData: DataFrame): Set[String] = {
        val ordDateColumnName = "ord_date"
        val ordDateColumn = col(ordDateColumnName)

        orderSummaryChangeData
            .select(ordDateColumn)
            .filter(ordDateColumn.isNotNull)
            .distinct
            .collect
            .map(_.getAs[java.sql.Date](ordDateColumnName).toString)
            .toSet
    }

    def readitem(itemTable: String)(implicit spark: SparkSession): DataFrame = {
        val itemDf = spark.table(itemTable)

        spark.sql(
            " create or replace temp view pos_item as" +
                " select item_id, cxi_partner_id, item_nm, item_desc, item_type, main_category_name, item_plu, item_barcode from refined_toast.item" +
                "\nUnion" +
                "\nselect item_id, cxi_partner_id, item_nm, item_desc, item_type, main_category_name, item_plu, item_barcode from refined_square.item" +
                "\nUnion" +
                "\nselect item_id, cxi_partner_id, item_nm, item_desc, item_type, main_category_name, item_plu, item_barcode from refined_omnivore.item" +
                "\nUnion" +
                "\nselect item_id, cxi_partner_id, item_nm, item_desc, item_type, main_category_name, item_plu, item_barcode from refined_parbrink.item"
        )
        spark.sql(
            "select item_id, cxi_partner_id, item_nm from pos_item where item_type != 'variation' and " +
                "item_nm is not null and item_nm not in ('Small', 'Large', 'Medium', 'Spicy', 'Plain', 'null', '') " +
                "and item_nm not rlike '^([Aa][Dd][Dd]|[Nn][Oo][Tt]?|[Ss][Ii]" +
                "[Dd][Ee]|[Ss][Uu][Bb]|[Ee][Xx][Tt][Rr][Aa]|[Ll][Ii][Gg][Hh][Tt]) |^[0-9]+ [Oo][Zz]$'"
        )

    }

    def readCustomer360(
        customer360table: String,
        orderSummaryTable: String,
        itemTable: String,
        itemuniversetable: String
    )(implicit spark: SparkSession): DataFrame = {

        val customer360 = spark.table(customer360table)
        val orderSummaryDF = spark.table(orderSummaryTable)
        val itemUniverseDf = spark.table(itemuniversetable)
        val itemDf = spark.table(itemTable)

        spark
            .sql("select customer_360_id, identities from $customer360table where active_flag = 1")
            .select(col("customer_360_id"), explode(col("identities")).as("type" :: "ids" :: Nil))
            .withColumn("id", explode(col("ids")))
            .select(col("customer_360_id"), concat(col("type"), lit(":"), col("id")).as("qualified_identity"))
        orderSummaryDF
            .select(col("*"), explode(col("cxi_identity_ids")).as("id"))
            .withColumn("qualified_identity", concat(col("id.identity_type"), lit(":"), col(s"id.cxi_identity_id")))
            .join(customer360, Seq("qualified_identity"), "inner") // Inner??
            .drop("qualified_identity", "cxi_identity_ids", "id")
            .dropDuplicates("ord_id", "cxi_partner_id", "location_id")
            .join(itemDf, Seq("cxi_partner_id", "item_id"), "inner")

    }

    def readOrderSummary(orderDates: Set[String], orderSummaryTable: String, locationTable: String, itemTable: String)(
        implicit spark: SparkSession
    ): DataFrame = {
        import spark.implicits._

        val orderSummaryDF = spark.table(orderSummaryTable)
        val locationDF = spark.table(locationTable)
        val getLocationTypeUdf = udf(getLocationType _)

        orderSummaryDF
            .filter($"ord_state_id" === OrderStateType.Completed.code && $"ord_date".isInCollection(orderDates))
            .join(locationDF, usingColumns = Seq("cxi_partner_id", "location_id"))
            .select(
                orderSummaryDF("cxi_partner_id"),
                getLocationTypeUdf(locationDF("location_type")).as("location_type"),
                locationDF("region"),
                upper(locationDF("state_code")).as("state"),
                initcap(locationDF("city")).as("city"),
                col("location_id"),
                col("location_nm"),
                orderSummaryDF("ord_date"),
                orderSummaryDF("ord_pay_total"),
                orderSummaryDF("ord_id")
            )
    }

    def getLocationType(locationTypeCode: Int): String = {
        LocationType.withValueOpt(locationTypeCode).getOrElse(LocationType.Unknown).name
    }

    def computePartnerItemInsights(orderSummary: DataFrame): DataFrame = {
        import orderSummary.sparkSession.implicits._

        orderSummary
            .groupBy(
                "ord_date",
                "cxi_partner_id",
                "location_type",
                "location_nm",
                "region",
                "state_code",
                "city",
                "item_nm"
            )
            .agg(
                countDistinct("ord_id") as "transaction_quantity",
                sum("item_quantity") as "item_quantity",
                sum("item_total") as "item_total",
                collect_set("customer_360_id") as "customer_360_ids"
            )
            .withColumnRenamed("ord_date", "date")
            .select(
                $"cxi_partner_id",
                $"location_type",
                $"region",
                $"state",
                $"city",
                $"location_id",
                $"location_nm",
                $"date",
                $"item_total",
                $"transaction_amount",
                $"transaction_quantity"
            )
    }

    def writeToDatalakePartnerItemInsights(
        partnerItemInsights: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            partnerItemInsights.sqlContext.sql(s"DELETE FROM $destTable")
        }
        val srcTable = "PartnerItemInsight"

        partnerItemInsights.createOrReplaceTempView(srcTable)

        partnerItemInsights.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destTable.date <=> $srcTable.date
               | AND $destTable.location_id <=> $srcTable.location_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoPartnerItemInsights(
        partnerItemInsights: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String,
        fullReprocess: Boolean = false
    ): Unit = {
        val saveMode = if (fullReprocess) SaveMode.Overwrite else SaveMode.Append

        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"cxi_partner_id": 1, "date": 1, "location_id": 1}"""

        partnerItemInsights.write
            .format(MongoSparkConnectorClass)
            .mode(saveMode)
            .option("database", dbName)
            .option("collection", collectionName)
            .option("uri", mongoDbUri)
            .option("replaceDocument", "true")
            .option("shardKey", shardKey)
            .save()
    }

    def computePartnerCategoryInsights(partnerItemInsights: DataFrame): DataFrame = {
        partnerItemInsights
            .groupBy(
                "ord_date",
                "cxi_partner_id",
                "location_type",
                "location_nm",
                "region",
                "state_code",
                "city",
                "item_category"
            )
            .agg(
                sum("transaction_quantity") as "transaction_quantity",
                sum("item_quantity") as "item_quantity",
                sum("item_total") as "item_total"
            )
            .select(
                "location_type",
                "region",
                "state",
                "city",
                "date",
                "item_quantity",
                "item_total",
                "transaction_quantity"
            )
    }

    def writeToDatalakePartnerCategoryInsights(
        partnerCategoryInsights: DataFrame,
        destTable: String,
        fullReprocess: Boolean = false
    ): Unit = {
        if (fullReprocess) {
            partnerCategoryInsights.sqlContext.sql(s"DELETE FROM $destTable")
        }

        val srcTable = "partnerCategoryInsight"
        partnerCategoryInsights.createOrReplaceTempView(srcTable)

        partnerCategoryInsights.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.location_type <=> $srcTable.location_type
               | AND $destTable.date <=> $srcTable.date
               | AND $destTable.region <=> $srcTable.region
               | AND $destTable.state <=> $srcTable.state
               | AND $destTable.city <=> $srcTable.city
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def writeToMongoPartnerCategoryInsights(
        partnerCategoryInsights: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String,
        fullReprocess: Boolean = false
    ): Unit = {
        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"date": 1, "location_type": 1, "region": 1, "state": 1, "city": 1}"""

        val saveMode = if (fullReprocess) SaveMode.Overwrite else SaveMode.Append
        val forceInsert = if (fullReprocess) true else false

        partnerCategoryInsights.write
            .format(MongoSparkConnectorClass)
            .mode(saveMode)
            .option("database", dbName)
            .option("collection", collectionName)
            .option("uri", mongoDbUri)
            .option("replaceDocument", "true")
            .option("shardKey", shardKey) // allow updates based on these fields
            .option("forceInsert", forceInsert)
            .save()
    }

    case class CliArgs(contractPath: String, fullReprocess: Boolean)
    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, fullReprocess = false)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Menu Item Insight Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, reprocess MLI fully from the beginning")
                .optional

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
