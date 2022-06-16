package com.cxi.cdp.data_processing
package refined_zone.hub.demographic

import refined_zone.hub.identity.model._
import refined_zone.service.MetadataService.extractMetadata
import support.{SparkSessionFactory, WorkspaceConfigReader}
import support.crypto_shredding.PrivacyFunctions.inAuthorizedContext
import support.utils.ContractUtils

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.nio.file.Paths

/** This Spark job extracts identity relationships from order summary and writes them into identity_relationship table.
  *
  * TODO: This will be updated on performance ticket DP-2251 once we have CDF ready
  */
object DemographicIdentityRelationshipsJob {

    private val logger = Logger.getLogger(this.getClass.getName)
    final val RelationshipType = "throtleRelated"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(cliArgs.contractPath))

        val rawDB = contract.prop[String](getSchemaRawPath("db_name"))
        val throtleEmailTable = contract.prop[String](getSchemaRawPath("tid_email_table"))
        val throtleIdfaTable = contract.prop[String](getSchemaRawPath("tid_idfa_table"))
        val throtleAaidTable = contract.prop[String](getSchemaRawPath("tid_aaid_table"))

        val lookupDB = contract.prop[String]("schema.crypto.db_name")
        val lookupTable = contract.prop[String]("schema.crypto.lookup_table")

        val refinedHubDB = contract.prop[String](getSchemaRefinedHubPath("db_name"))
        val demographicIdentityTable = contract.prop[String](getSchemaRefinedHubPath("demographic_identity_table"))
        val demographicIdentityRelationshipTable =
            contract.prop[String](getSchemaRefinedHubPath("demographic_identity_relationship_table"))

        val workspaceConfigPath: String = contract.prop[String]("databricks_workspace_config")
        val workspaceConfig = WorkspaceConfigReader.readWorkspaceConfig(spark, workspaceConfigPath)

        val throtleEmailTableName = s"$rawDB.$throtleEmailTable"
        val dfThrotleEmail =
            readThrotleTidEmail(throtleEmailTableName, findMaxFeedDateInRawTable(throtleEmailTableName))
        val throtleIdfaTableName = s"$rawDB.$throtleIdfaTable"
        val dfThrotleIDFA = readThrotleTidIdfa(throtleIdfaTableName, findMaxFeedDateInRawTable(throtleIdfaTableName))
        val throtleAaidTableName = s"$rawDB.$throtleAaidTable"
        val dfThrotleAAID = readThrotleTidAaid(throtleAaidTableName, findMaxFeedDateInRawTable(throtleAaidTableName))

        inAuthorizedContext(spark, workspaceConfig) {
            val privacyLookupDf = readPrivacyLookupTableEmails(s"$lookupDB.$lookupTable")

            val transformedThrotleMaids = transformThrotleMaids(dfThrotleIDFA, dfThrotleAAID)
            val transformedThrotleEmail = transformThrotleEmail(dfThrotleEmail, privacyLookupDf)

            val transformedDemographicIdentity =
                transformDemographicIdentity(transformedThrotleMaids, transformedThrotleEmail)
            writeDemographicIdentity(transformedDemographicIdentity, s"$refinedHubDB.$demographicIdentityTable")

            val transformedDemographicIdentityRelationship =
                transformDemographicIdentityRelationship(
                    transformedThrotleMaids,
                    transformedThrotleEmail,
                    cliArgs.runDate
                )
            writeDemographicIdentityRelationship(
                transformedDemographicIdentityRelationship,
                s"$refinedHubDB.$demographicIdentityRelationshipTable"
            )
        }
    }

    def findMaxFeedDateInRawTable(table: String)(implicit spark: SparkSession): String = {
        val row = spark.sql(s"SELECT MAX(feed_date) as feed_date FROM $table").first()
        val feedDate = row.getAs[String]("feed_date")
        Option(feedDate).getOrElse(throw new IllegalStateException(s"There is no data in raw table $table"))
    }

    def readThrotleTidEmail(tableName: String, feedDate: String)(implicit spark: SparkSession): DataFrame = {
        spark.sql(s"""
              SELECT throtle_id, sha256_lower_email
              FROM $tableName
              WHERE feed_date = "$feedDate"
        """.stripMargin)
    }

    def readThrotleTidIdfa(tableName: String, feedDate: String)(implicit spark: SparkSession): DataFrame = {
        spark.sql(s"""
              SELECT throtle_id, native_maid
              FROM $tableName
              WHERE feed_date = "$feedDate"
        """.stripMargin)
    }

    def readThrotleTidAaid(tableName: String, feedDate: String)(implicit spark: SparkSession): DataFrame =
        readThrotleTidIdfa(tableName, feedDate) // as of now has same schema as tid_idfa dataset

    def readPrivacyLookupTableEmails(tableName: String)(implicit spark: SparkSession): DataFrame = {
        spark
            .sql(
                s"""
               |SELECT original_value, sha2(original_value, "256") AS lookup_email , hashed_value, identity_type
               |FROM $tableName
               |WHERE identity_type = "${IdentityType.Email.code}"
               |""".stripMargin
            )
            .dropDuplicates("hashed_value", "identity_type")
    }

    def transformThrotleMaids(throtleIdfa: DataFrame, throtleAaid: DataFrame): DataFrame = {
        val transformedThrotleIdfa = throtleIdfa
            .filter(col("native_maid").isNotNull)
            .dropDuplicates("throtle_id", "native_maid") // since we read from raw zone, we might have duplicates
            .withColumn("maid_identity_type", lit(IdentityType.MaidIDFA.code))

        val transformedThrotleAaid = throtleAaid
            .filter(col("native_maid").isNotNull)
            .dropDuplicates("throtle_id", "native_maid") // since we read from raw zone, we might have duplicates
            .withColumn("maid_identity_type", lit(IdentityType.MaidAAID.code))

        transformedThrotleIdfa.unionByName(transformedThrotleAaid)
    }

    def transformThrotleEmail(throtleEmail: DataFrame, privacyLookupEmails: DataFrame): DataFrame = {
        val throtleEmailsWithoutDuplicates = throtleEmail
            .filter(col("sha256_lower_email").isNotNull)
            .dropDuplicates("throtle_id", "sha256_lower_email") // since we read from raw zone, we might have duplicates

        val extractEmailMetadataUdf = udf(extractMetadata _)

        throtleEmailsWithoutDuplicates
            .join( // filter out emails we don't have and get out hash-salted email
                broadcast(privacyLookupEmails),
                throtleEmailsWithoutDuplicates("sha256_lower_email") === privacyLookupEmails("lookup_email"),
                "inner"
            )
            .select(
                col("throtle_id"),
                col("hashed_value").as("hash_salted_email"),
                extractEmailMetadataUdf(lit(IdentityType.Email.code), col("original_value")).as("email_metadata")
            )
    }

    def transformDemographicIdentity(
        transformedThrotleMaids: DataFrame,
        transformedThrotleEmail: DataFrame
    ): DataFrame = {
        val transformedMaidDemographicIdentities = transformedThrotleMaids
            .withColumn("throtle_id_identity_type", lit(IdentityType.ThrotleId.code))
            .select(
                expr(
                    """STACK(2,
                      |throtle_id, throtle_id_identity_type,
                      |native_maid, maid_identity_type
                      |) AS (cxi_identity_id, type)""".stripMargin
                ),
                typedLit(Map.empty[String, String]).as("metadata")
            )

        val transformedEmailDemographicIdentities = transformedThrotleEmail
            .withColumn("throtle_id_identity_type", lit(IdentityType.ThrotleId.code))
            .withColumn("throtle_id_metadata", typedLit(Map.empty[String, String]))
            .withColumn("email_identity_type", lit(IdentityType.Email.code))
            .select(
                expr(
                    """STACK(2,
                      |throtle_id, throtle_id_identity_type, throtle_id_metadata,
                      |hash_salted_email, email_identity_type, email_metadata
                      |) AS (cxi_identity_id, type, metadata)""".stripMargin
                )
            )

        transformedMaidDemographicIdentities
            .unionByName(transformedEmailDemographicIdentities)
            .dropDuplicates("cxi_identity_id", "type")
    }

    def writeDemographicIdentity(df: DataFrame, targetTable: String): Unit = {
        val srcTable = "newDemographicIdentity"
        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $targetTable
               |USING $srcTable
               |ON $targetTable.cxi_identity_id <=> $srcTable.cxi_identity_id
               |   AND $targetTable.type <=> $srcTable.type
               |WHEN NOT MATCHED
               |  THEN INSERT *
            """.stripMargin)
    }

    def transformDemographicIdentityRelationship(
        transformedThrotleMaids: DataFrame,
        transformedThrotleEmail: DataFrame,
        date: String
    ): DataFrame = {
        val transformedMaidDemographicIdentityRelationships = transformedThrotleMaids
            .withColumn("throtle_id_identity_type", lit(IdentityType.ThrotleId.code))
            .select(
                col("throtle_id").as("source"),
                lit(IdentityType.ThrotleId.code).as("source_type"),
                col("native_maid").as("target"),
                col("maid_identity_type").as("target_type")
            )

        val transformedEmailDemographicIdentityRelationships = transformedThrotleEmail
            .select(
                col("throtle_id").as("source"),
                lit(IdentityType.ThrotleId.code).as("source_type"),
                col("hash_salted_email").as("target"),
                lit(IdentityType.Email.code).as("target_type")
            )

        transformedMaidDemographicIdentityRelationships
            .unionByName(transformedEmailDemographicIdentityRelationships)
            .dropDuplicates("source", "source_type", "target", "target_type")
            .withColumn("relationship", lit(RelationshipType))
            .withColumn("frequency", lit(1))
            .withColumn("created_date", lit(date))
            .withColumn("last_seen_date", lit(date))
            .withColumn("active_flag", lit(true))
    }

    def writeDemographicIdentityRelationship(df: DataFrame, targetTable: String): Unit = {
        val srcTable = "newDemographicIdentityRelationship"
        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $targetTable
               |USING $srcTable
               |ON $targetTable.source <=> $srcTable.source
               |  AND $targetTable.source_type <=> $srcTable.source_type
               |  AND $targetTable.target <=> $srcTable.target
               |  AND $targetTable.target_type <=> $srcTable.target_type
               |WHEN MATCHED THEN UPDATE SET
               |  created_date = ARRAY_MIN(ARRAY($targetTable.created_date, $srcTable.created_date)),
               |  last_seen_date = ARRAY_MAX(ARRAY($targetTable.last_seen_date, $srcTable.last_seen_date))
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def getSchemaRawPath(relativePath: String): String = s"schema.raw.$relativePath"

    def getSchemaRefinedHubPath(relativePath: String): String = s"schema.refined_hub.$relativePath"

    case class CliArgs(contractPath: String = null, runDate: String)

    object CliArgs {

        private val initOptions = CliArgs(runDate = null)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Update Demographic Identity Relationships Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[String]("run-date")
                .action((runDate, c) => c.copy(runDate = runDate))
                .text("run date")
                .required
        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
