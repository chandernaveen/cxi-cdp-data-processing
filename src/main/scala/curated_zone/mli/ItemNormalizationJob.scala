package com.cxi.cdp.data_processing
package curated_zone.mli

import refined_zone.hub.ChangeDataFeedViews
import support.utils.mongodb.MongoDbConfigUtils
import support.utils.mongodb.MongoDbConfigUtils.MongoSparkConnectorClass
import support.utils.ContractUtils
import support.SparkSessionFactory

import me.xdrop.fuzzywuzzy.FuzzySearch
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.Instant

object ItemNormalizationJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    final val CdfConsumerId = "item_normalization_job"
    final val scoreThreshold = 82

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

        val itemTables = contract.prop[Seq[String]]("schema.item_tables")

        val itemCdf = ChangeDataFeedViews.item(s"$dataServicesDb.$cdfTrackerTable", itemTables)

        val itemChangeDataResult =
            if (cliArgs.fullReprocess) {
                itemCdf.queryAllData(CdfConsumerId)
            } else {
                itemCdf.queryChangeData(CdfConsumerId)
            }

        itemChangeDataResult.data match {
            case None => logger.info("No updates found since the last run")

            case Some(changeData) =>
                val partnerItems = getPartnerItemsToProcess(changeData)
                if (partnerItems.isEmpty) {
                    logger.info(s"No partner items to process")
                } else {
                    val partners = partnerItems.select("cxi_partner_id").distinct.collect.mkString(",")
                    logger.info(s"Partners to process: $partners")
                    process(contract, partnerItems, cliArgs.fullReprocess)
                }

                logger.info(s"Update CDF tracker: ${itemChangeDataResult.tableMetadataSeq}")
                itemCdf.markProcessed(itemChangeDataResult)
        }
    }

    def process(contract: ContractUtils, partnerItems: DataFrame, fullReprocess: Boolean)(implicit
        spark: SparkSession
    ): Unit = {
        val refinedHubCxiTaxonomyDb = contract.prop[String]("schema.refined_hub_cxi_taxonomy.db_name")
        val itemCategoryTable = contract.prop[String]("schema.refined_hub_cxi_taxonomy.item_category_table")

        val curatedDb = contract.prop[String]("schema.curated.db_name")
        val itemUniverseTable = contract.prop[String]("schema.curated.item_universe_table")

        val mongoDbConfig = MongoDbConfigUtils.getMongoDbConfig(spark, contract)
        val mongoDbName = contract.prop[String]("mongo.db")
        val itemUniverseMongoCollectionName = contract.prop[String]("mongo.item_universe_collection")

        val itemCategoryTaxonomy: DataFrame =
            readItemCategoryTaxonomy(s"$refinedHubCxiTaxonomyDb.$itemCategoryTable")

        val processTime = Timestamp.from(Instant.now())
        val partnerItemCategory: DataFrame = getPartnerItemCategory(partnerItems, itemCategoryTaxonomy, processTime)

        writeToDatalakeItemUniverse(
            partnerItemCategory,
            s"$curatedDb.$itemUniverseTable"
        )

        writeToMongoItemUniverse(
            partnerItemCategory,
            mongoDbConfig.uri,
            mongoDbName,
            itemUniverseMongoCollectionName
        )
    }

    def getPartnerItemsToProcess(itemDf: DataFrame): DataFrame = {

        val excludeItemNames = Set("Small", "Large", "Medium", "Spicy", "Plain", "null", "")
        val excludeItemNamesRegex =
            "^([Aa][Dd][Dd]|[Nn][Oo][Tt]?|[Ss][Ii][Dd][Ee]|[Ss][Uu][Bb]|[Ee][Xx][Tt][Rr][Aa]|[Ll][Ii][Gg][Hh][Tt]) |^[0-9]+ [Oo][Zz]$"

        itemDf
            .filter(
                (col("item_type").rlike("[Ff][Oo][Oo][Dd]|Normal")) && col("item_nm").isNotNull && !col("item_nm")
                    .isInCollection(
                        excludeItemNames
                    ) && !col("item_nm").rlike(excludeItemNamesRegex)
            )
            .select("cxi_partner_id", "item_nm")
            .dropDuplicates()
    }

    def readItemCategoryTaxonomy(itemCategoryTaxonomyTable: String)(implicit
        spark: SparkSession
    ): DataFrame = {
        spark
            .table(itemCategoryTaxonomyTable)
            .dropDuplicates()
    }

    def getPartnerItemCategory(
        partnerItems: DataFrame,
        itemCategoryTaxonomy: DataFrame,
        processTime: Timestamp
    ): DataFrame = {

        val fuzzyUdf = udf(fuzzySearch _)

        // UDF join are only inner
        val partnerItemsMapped = partnerItems
            .join(broadcast(itemCategoryTaxonomy), fuzzyUdf(col("item_nm"), col("cxi_item_nm")) > scoreThreshold)
            // .withColumn("score", fuzzyUdf($"item_nm", $"cxi_item_nm"))
            .withColumn("cxi_item_category", explode(col("cxi_item_categories")))
            .groupBy("cxi_partner_id", "item_nm")
            .agg(sort_array(collect_set(col("cxi_item_category"))).as("cxi_item_categories"))

        // Non joined partner items
        val partnerItemsNotMapped = partnerItems
            .join(partnerItemsMapped, Seq("cxi_partner_id", "item_nm"), "left_anti")
            .withColumn("cxi_item_category", lit("Unknown"))
            .withColumnRenamed("item_nm", "pos_item_nm")
            .select("cxi_partner_id", "cxi_item_category", "pos_item_nm")

        // Partner items categories
        partnerItemsMapped
            .withColumn("cxi_item_category", explode(col("cxi_item_categories")))
            .withColumnRenamed("item_nm", "pos_item_nm")
            .drop("item_nm", "cxi_item_categories")
            .unionByName(partnerItemsNotMapped)
            .withColumn("updated_on", lit(processTime))
            .select(
                "cxi_partner_id",
                "cxi_item_category",
                "pos_item_nm",
                "updated_on"
            )
            .orderBy("cxi_partner_id", "cxi_item_category", "pos_item_nm")
    }

    def fuzzySearch(original_title: String, title: String): Int = {
        FuzzySearch.tokenSetRatio(original_title, title)
    }

    def writeToDatalakeItemUniverse(
        partnerItemCategory: DataFrame,
        destTable: String
    ): Unit = {
        val srcTable = "newPartnerItemCategory"

        partnerItemCategory.createOrReplaceTempView(srcTable)

        // <=> comparison operator is used for null-safe semantics
        // https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-null-semantics.html#null-semantics
        partnerItemCategory.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               | AND $destTable.cxi_item_category <=> $srcTable.cxi_item_category
               | AND $destTable.pos_item_nm <=> $srcTable.pos_item_nm
               |WHEN MATCHED
               |  THEN UPDATE SET
               |    cxi_item_category = $srcTable.cxi_item_category,
               |    updated_on = $srcTable.updated_on
               |WHEN NOT MATCHED
               |  THEN INSERT (
               |    cxi_partner_id,
               |    cxi_item_category,
               |    pos_item_nm,
               |    item_category_modified,
               |    item_nm_modified,
               |    is_category_modified,
               |    is_item_modified,
               |    updated_on
               |  )
               |  VALUES (
               |    $srcTable.cxi_partner_id,
               |    $srcTable.cxi_item_category,
               |    $srcTable.pos_item_nm,
               |    null,
               |    null,
               |    false,
               |    false,
               |    $srcTable.updated_on
               |  )
               |""".stripMargin)
    }

    def writeToMongoItemUniverse(
        partnerItemCategory: DataFrame,
        mongoDbUri: String,
        dbName: String,
        collectionName: String
    ): Unit = {
        // either insert or update a document in Mongo based on these fields
        val shardKey = """{"cxi_partner_id": 1, "cxi_item_category": 1, "pos_item_nm": 1}"""

        partnerItemCategory.write
            .format(MongoSparkConnectorClass)
            .mode(SaveMode.Append)
            .option("database", dbName)
            .option("collection", collectionName)
            .option("uri", mongoDbUri)
            .option("replaceDocument", "false")
            .option("shardKey", shardKey)
            .save()
    }

    case class CliArgs(contractPath: String, fullReprocess: Boolean)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, fullReprocess = false)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Item Normalization Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, reprocess Item Normalization fully from the beginning")
                .optional

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
