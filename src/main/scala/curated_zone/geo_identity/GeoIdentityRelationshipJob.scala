package com.cxi.cdp.data_processing
package curated_zone.geo_identity

import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import java.nio.file.Paths
import java.time.LocalDate

/** This Spark job creates geo identity relationships and related data.
  *
  * 1. Read geo_location_to_orders table created by GeoLocationToOrderJoin job.
  * 2. Join geo_location_to_orders with pos_customer_360 to join geo_location data with customers.
  * 3. Calculate confidence score for geo_location_to_customer and write it to the Data Lake.
  * 4. Take geo_location - customer relationships with high enough confidence scores and use them to create
  *    geo_identity and geo_identity_relationship tables.
  */
object GeoIdentityRelationshipJob {

    final val RelationshipType = "geoRelated"

    private final val JobConfigPrefix = "jobs.databricks.geo_identity_relationship_job.job_config"

    private val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        val spark = SparkSessionFactory.getSparkSession()
        run(cliArgs)(spark)
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(cliArgs.contractPath))

        val geoLocationToOrderTable = getGeoLocationToOrderTable(contract)
        val posCustomer360Table = getPosCustomer360Table(contract)

        val geoLocationToCustomerTable = getGeoLocationToCustomerTable(contract)
        val geoIdentityTable = getGeoIdentityTable(contract)
        val geoIdentityRelationshipTable = getGeoIdentityRelationshipTable(contract)

        val minFrequencyLinked = getMinFrequencyLinked(contract)
        val confidenceScoreCutoff = getConfidenceScoreCutoff(contract)
        val orderDateStart: LocalDate = getOrderDateStart(cliArgs.processingDate, getOrderLookbackDays(contract))

        val geoLocationToOrder = readGeoLocationToOrder(geoLocationToOrderTable, orderDateStart)
        val posCustomer360 = readActivePosCustomer360(posCustomer360Table)

        val geoLocationToCustomer = joinGeoLocationWithCustomer(geoLocationToOrder, posCustomer360)
        val geoLocationToCustomerWithConfidenceScore =
            calculateConfidenceScore(geoLocationToCustomer, minFrequencyLinked)

        writeGeoLocationToCustomer(geoLocationToCustomerWithConfidenceScore, geoLocationToCustomerTable)

        val geoLocationToCustomerScoreCutoff = filterByConfidenceScoreCutoff(
            geoLocationToCustomer = readGeoLocationToCustomer(geoLocationToCustomerTable),
            confidenceScoreCutoff = confidenceScoreCutoff
        )

        val geoIdentity = extractGeoIdentity(geoLocationToCustomerScoreCutoff)
        writeGeoIdentity(geoIdentity, geoIdentityTable)

        val geoIdentityRelationship = createGeoIdentityRelationship(
            geoLocationToCustomerScoreCutoff,
            posCustomer360,
            currentDate = cliArgs.processingDate
        )
        writeGeoIdentityRelationship(geoIdentityRelationship, geoIdentityRelationshipTable)
    }

    private[geo_identity] def joinGeoLocationWithCustomer(geoLocationToOrder: DataFrame, posCustomer360: DataFrame)(
        implicit spark: SparkSession
    ): DataFrame = {
        import spark.implicits._

        val geoLocationToOrderExploded = geoLocationToOrder
            .filter($"cxi_identity_ids".isNotNull)
            .select(
                col("*"),
                explode($"cxi_identity_ids").as("cxi_identity_id"),
                $"cxi_identity_id.cxi_identity_id".as("identity_id"),
                $"cxi_identity_id.identity_type".as("identity_type")
            )

        val posCustomer360Exploded = posCustomer360
            .select(col("*"), explode(col("identities")).as(Seq("identity_type", "identity_values_array")))
            .withColumn("identity_id", explode(col("identity_values_array")))

        geoLocationToOrderExploded
            .join(posCustomer360Exploded, Seq("identity_type", "identity_id"), "inner")
            .dropDuplicates("maid", "maid_type", "customer_360_id", "ord_id", "cxi_partner_id", "location_id")
            .select($"maid", $"maid_type", $"customer_360_id", $"device_score")
            .groupBy($"maid", $"maid_type", $"customer_360_id")
            .agg(count("*").cast(IntegerType).as("frequency_linked"), sum("device_score").as("profile_maid_link_score"))
            .select(
                $"maid",
                $"maid_type",
                $"customer_360_id",
                $"frequency_linked",
                $"profile_maid_link_score"
            )
    }

    private[geo_identity] def calculateConfidenceScore(geoLocationToCustomer: DataFrame, minFrequencyLinked: Int)(
        implicit spark: SparkSession
    ): DataFrame = {
        import spark.implicits._

        val windowMaid = Window.partitionBy("maid_type", "maid")
        val windowProfile = Window.partitionBy("customer_360_id")

        geoLocationToCustomer
            .withColumn("total_profiles_per_maid", count("*") over windowMaid)
            .withColumn("total_score_per_maid", sum("profile_maid_link_score") over windowMaid)
            .withColumn("total_maids_per_profile", count("*") over windowProfile)
            .withColumn("total_score_per_profile", sum("profile_maid_link_score") over windowProfile)
            .withColumn(
                "confidence_score",
                when($"frequency_linked" >= minFrequencyLinked, 1.0).otherwise(0.0) *
                    ($"frequency_linked" / $"total_score_per_profile") *
                    ($"profile_maid_link_score" / $"total_score_per_profile") *
                    pow($"profile_maid_link_score" / $"total_score_per_maid", 0.1)
            )
    }

    private def getGeoLocationToOrderTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.geo_location_to_order_table")
        s"$db.$table"
    }

    private def getPosCustomer360Table(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.curated_hub.db_name")
        val table = contract.prop[String]("schema.curated_hub.pos_customer_360_table")
        s"$db.$table"
    }

    private def getGeoLocationToCustomerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.curated_hub.db_name")
        val table = contract.prop[String]("schema.curated_hub.geo_location_to_customer_table")
        s"$db.$table"
    }

    private def getGeoIdentityTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.curated_hub.db_name")
        val table = contract.prop[String]("schema.curated_hub.geo_identity_table")
        s"$db.$table"
    }

    private def getGeoIdentityRelationshipTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.curated_hub.db_name")
        val table = contract.prop[String]("schema.curated_hub.geo_identity_relationship_table")
        s"$db.$table"
    }

    private def getMinFrequencyLinked(contract: ContractUtils): Int = {
        contract.prop[Int](s"$JobConfigPrefix.min_frequency_linked")
    }

    private def getConfidenceScoreCutoff(contract: ContractUtils): Double = {
        contract.prop[Double](s"$JobConfigPrefix.confidence_score_cutoff")
    }

    private[geo_identity] def getOrderDateStart(currentDate: LocalDate, orderLookbackDays: Int): LocalDate = {
        currentDate.minusDays(orderLookbackDays.toLong - 1L)
    }

    private def getOrderLookbackDays(contract: ContractUtils): Int = {
        contract.prop[Int](s"$JobConfigPrefix.order_lookback_days")
    }

    private def readGeoLocationToOrder(geoLocationToOrderTable: String, orderDateStart: LocalDate)(implicit
        spark: SparkSession
    ): DataFrame = {
        import spark.implicits._

        val orderDateStartSql = java.sql.Date.valueOf(orderDateStart)

        spark
            .table(geoLocationToOrderTable)
            .filter($"ord_date" >= orderDateStartSql)
    }

    private def readActivePosCustomer360(posCustomer360Table: String)(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        spark.table(posCustomer360Table).filter($"active_flag" === true)
    }

    private[geo_identity] def writeGeoLocationToCustomer(
        geoLocationToCustomer: DataFrame,
        targetTable: String
    )(implicit spark: SparkSession): Unit = {
        geoLocationToCustomer.write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .saveAsTable(targetTable)
    }

    private[geo_identity] def writeGeoIdentity(
        geoIdentity: DataFrame,
        targetTable: String
    )(implicit spark: SparkSession): Unit = {
        geoIdentity.write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .saveAsTable(targetTable)
    }

    private[geo_identity] def writeGeoIdentityRelationship(
        geoIdentityRelationship: DataFrame,
        targetTable: String
    )(implicit spark: SparkSession): Unit = {
        geoIdentityRelationship.write
            .format("delta")
            .mode(SaveMode.Overwrite)
            .saveAsTable(targetTable)
    }

    private def readGeoLocationToCustomer(
        geoLocationToCustomerTable: String
    )(implicit spark: SparkSession): DataFrame = {
        spark.table(geoLocationToCustomerTable)
    }

    private[geo_identity] def filterByConfidenceScoreCutoff(
        geoLocationToCustomer: DataFrame,
        confidenceScoreCutoff: Double
    )(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._
        geoLocationToCustomer.filter($"confidence_score" > confidenceScoreCutoff)
    }

    private[geo_identity] def extractGeoIdentity(
        geoLocationToCustomer: DataFrame
    )(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        geoLocationToCustomer
            .select(
                $"maid".as(CxiIdentity.CxiIdentityId),
                $"maid_type".as(CxiIdentity.Type)
            )
            .distinct
            .withColumn(CxiIdentity.Weight, typedLit[String](null))
            .withColumn(CxiIdentity.Metadata, typedLit[Map[String, String]](null))
    }

    private[geo_identity] def createGeoIdentityRelationship(
        geoLocationToCustomer: DataFrame,
        posCustomer360: DataFrame,
        currentDate: LocalDate
    )(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        val currentDateSql = java.sql.Date.valueOf(currentDate)

        geoLocationToCustomer
            .join(posCustomer360, Seq("customer_360_id"), "inner")
            .select(
                col("*"),
                explode(col("identities")).as(Seq("customer_identity_type", "customer_identity_values_array"))
            )
            .withColumn("customer_identity_id", explode(col("customer_identity_values_array")))
            .select(
                $"maid".as("source"),
                $"maid_type".as("source_type"),
                $"customer_identity_id".as("target"),
                $"customer_identity_type".as("target_type"),
                lit(RelationshipType).as("relationship"),
                $"frequency_linked".as("frequency"),
                lit(currentDateSql).as("created_date"),
                lit(currentDateSql).as("last_seen_date"),
                lit(true).as("active_flag")
            )
    }

    case class CliArgs(contractPath: String, processingDate: LocalDate)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, processingDate = null)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Geo Identity Relationship Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("A path to a contract for this job")
                .required

            opt[String]("processing-date")
                .action((rawProcessingDate, c) => c.copy(processingDate = LocalDate.parse(rawProcessingDate)))
                .text(
                    "A date when this job run is started. Should be in ISO format. " +
                        "Used as the 'current date' when populating identity relationship table."
                )
                .required

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
