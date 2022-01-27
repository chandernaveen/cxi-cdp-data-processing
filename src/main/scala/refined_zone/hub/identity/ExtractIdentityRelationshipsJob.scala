package com.cxi.cdp.data_processing
package refined_zone.hub.identity

import java.nio.file.Paths

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model._
import com.cxi.cdp.data_processing.refined_zone.hub.ChangeDataFeedViews
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity
import com.cxi.cdp.data_processing.support.SparkSessionFactory
import com.cxi.cdp.data_processing.support.change_data_feed.ChangeDataFeedService.ChangeType
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{coalesce, col, lit, size}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** This Spark job extracts identity relationships from order summary and writes them into identity_relationship table.
  *
  * The job can run in two modes:
  * 1. (default) Query newly added orders (using Change Data Feed), extract identity relationships,
  * and merge them into identity_relationship table.
  * In this mode we are looking at "inserted" records only. In theory it is possible to handle updates
  * as well, but the logic becomes more complicated (will be addressed in another ticket).
  * If there are updates, it's better to do a full reprocess for now.
  * 2. Full reprocess. Remove existing identity relationships, get all available orders,
  * and then import identity relationships from them.
  */
object ExtractIdentityRelationshipsJob {

    final val CdfConsumerId = "extract_identity_relationships_job"

    final val RelationshipType = "posRelated"

    private val logger = Logger.getLogger(this.getClass.getName)

    private val dateOrdering: Ordering[java.sql.Date] = Ordering.by(_.getTime)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(cliArgs.contractPath))

        val cdfTrackerTable = getCdfTrackerTable(contract)
        val orderSummaryTables = getOrderSummaryTables(contract)
        val identityRelationshipTable = getPosIdentityRelationshipTable(contract)

        if (cliArgs.fullReprocess) {
            logger.info("Full reprocess is requested, removing existing identity relationships")
            removeIdentityRelationships(identityRelationshipTable)
        }

        val orderSummaryCdf = ChangeDataFeedViews.orderSummary(cdfTrackerTable, orderSummaryTables)

        val orderSummaryChangeDataResult =
            if (cliArgs.fullReprocess) {
                orderSummaryCdf.queryAllData(CdfConsumerId)
            } else {
                orderSummaryCdf.queryChangeData(CdfConsumerId, changeTypes = Seq(ChangeType.Insert))
            }

        orderSummaryChangeDataResult.data match {
            case None => logger.info("No updates found since the last run")

            case Some(changeData) =>
                val relatedIdentities = extractRelatedEntities(changeData)

                val identityRelationships = createIdentityRelationships(relatedIdentities)
                writeIdentityRelationships(identityRelationships, identityRelationshipTable)

                logger.info(s"Update CDF tracker: ${orderSummaryChangeDataResult.tableMetadataSeq}")
                orderSummaryCdf.markProcessed(orderSummaryChangeDataResult)
        }
    }

    private def getCdfTrackerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.data_services.db_name")
        val table = contract.prop[String]("schema.data_services.cdf_tracker_table")
        s"$db.$table"
    }

    private def getOrderSummaryTables(contract: ContractUtils): Seq[String] = {
        val refinedSquareDb = contract.prop[String]("schema.refined_square.db_name")
        val orderSummaryTable = contract.prop[String]("schema.refined_square.order_summary_table")
        Seq(s"$refinedSquareDb.$orderSummaryTable")
    }

    private def getPosIdentityRelationshipTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.pos_identity_relationship_table")
        s"$db.$table"
    }

    private def removeIdentityRelationships(identityRelationshipTable: String)(implicit spark: SparkSession): Unit = {
        spark.sql(s"DELETE FROM $identityRelationshipTable")
    }

    private[identity] def extractRelatedEntities(orderSummaryDF: DataFrame)(implicit spark: SparkSession): Dataset[RelatedIdentities] = {
        import spark.implicits._

        val ordDateFallback = new java.sql.Date(java.time.Instant.now().toEpochMilli)

        orderSummaryDF
            .filter(size(col(CxiIdentity.CxiIdentityIds)) > 1)
            .select(
                col(CxiIdentity.CxiIdentityIds).as("identities"),
                coalesce(col("ord_date"), lit(ordDateFallback)).as("date")
            )
            .as[RelatedIdentities]
    }

    /** Creates a dataset of identity relationships from a dataset of related identities.
      *
      * The output dataset will not have duplicates based on source/source_type/target/target_type,
      * all duplicate records will be merged.
      */
    private[identity] def createIdentityRelationships(
                                          relatedIdentitiesDS: Dataset[RelatedIdentities]
                                      )(implicit spark: SparkSession): Dataset[IdentityRelationship] = {
        import spark.implicits._

        relatedIdentitiesDS
            .flatMap(relatedIdentities => createIdentityRelationships(relatedIdentities))
            .groupByKey(r => (r.source, r.source_type, r.target, r.target_type))
            .reduceGroups(mergeIdentityRelationships _)
            .map(_._2)
    }

    /** Creates identity relationships for a single case of related identities.
      *
      * When we see identities grouped together, we consider them related, and create an identity relationship
      * for every combination of two drawn from these identities
      *
      * Note that for combinations - in the mathematical sense - the order does not matter.
      * Thus, for deterministic results, identities are sorted before drawing combinations.
      */
    private def createIdentityRelationships(relatedIdentities: RelatedIdentities): Iterator[IdentityRelationship] = {
        // sort identities so that source-target order is deterministically defined
        val sortedIdentities = relatedIdentities
            .identities
            .sorted(IdentityId.SourceToTargetOrdering)
            .distinct

        sortedIdentities
            .combinations(2)
            .map({ case Seq(sourceIdentity, targetIdentity) =>
                IdentityRelationship(
                    source = sourceIdentity.cxi_identity_id,
                    source_type = sourceIdentity.identity_type,
                    target = targetIdentity.cxi_identity_id,
                    target_type = targetIdentity.identity_type,
                    relationship = RelationshipType,
                    frequency = 1,
                    created_date = relatedIdentities.date,
                    last_seen_date = relatedIdentities.date,
                    active_flag = true
                )
            })
    }

    private def writeIdentityRelationships(ds: Dataset[IdentityRelationship], destTable: String): Unit = {
        val df = ds.toDF

        val srcTable = "newIdentityRelationsips"
        df.createOrReplaceTempView(srcTable)

        df.sqlContext.sql(
            s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.source <=> $srcTable.source
               |  AND $destTable.source_type <=> $srcTable.source_type
               |  AND $destTable.target <=> $srcTable.target
               |  AND $destTable.target_type <=> $srcTable.target_type
               |WHEN MATCHED
               |  THEN UPDATE SET
               |    frequency = $destTable.frequency + $srcTable.frequency,
               |    created_date = ARRAY_MIN(ARRAY($destTable.created_date, $srcTable.created_date)),
               |    last_seen_date = ARRAY_MAX(ARRAY($destTable.last_seen_date, $srcTable.last_seen_date)),
               |    active_flag = $destTable.active_flag AND $srcTable.active_flag
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    /** Merges together two IdentityRelationship record.
      * It is assumed that these records have the same key (source/source_type/target/target_type).
      */
    private[identity] def mergeIdentityRelationships(
                                                        first: IdentityRelationship,
                                                        second: IdentityRelationship): IdentityRelationship = {
        first.copy(
            frequency = first.frequency + second.frequency,
            created_date = dateOrdering.min(first.created_date, second.created_date),
            last_seen_date = dateOrdering.max(first.last_seen_date, second.last_seen_date),
            active_flag = first.active_flag && second.active_flag
        )
    }

    case class CliArgs(contractPath: String, fullReprocess: Boolean = false)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Update Identity Relationships Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, remove current indentity relationships and reprocess them from the beginning")

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser.parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}