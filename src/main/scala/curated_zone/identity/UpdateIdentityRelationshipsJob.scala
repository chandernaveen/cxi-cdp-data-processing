package com.cxi.cdp.data_processing
package curated_zone.identity

import java.nio.file.Paths

import com.cxi.cdp.data_processing.curated_zone.identity.model._
import com.cxi.cdp.data_processing.refined_zone.hub.ChangeDataFeedViews
import com.cxi.cdp.data_processing.support.SparkSessionFactory
import com.cxi.cdp.data_processing.support.change_data_feed.ChangeDataFeedService.{ChangeType, ChangeTypeColumn}
import com.cxi.cdp.data_processing.support.packages.utils.ContractUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, size}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** This Spark job extracts identity relationships from order summary and writes them into identity_relationship table.
  *
  * The job can run in two modes:
  * 1. (default) Query newly added orders (using Change Data Feed), extract identity relationships,
  *    and merge them into identity_relationship table.
  *    In this mode we are looking at "inserted" records only. In theory it is possible to handle updates
  *    as well, but the logic becomes too complicated. If there are updates, it's better to do a full reprocess.
  * 2. Full reprocess. Remove existing identity relationships, get all available orders,
  *    and then import identity relationships from them.
  */
object UpdateIdentityRelationshipsJob {

    final val CdfConsumerId = "update_identity_relationships_job"

    final val RelationshipType = "pos_related"

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
        val identityRelationshipTable = getIdentityRelationshipTable(contract)

        if (cliArgs.fullReprocess) {
            logger.info("Full reprocess is requested, removing existing identity relationships")
            removeIdentityRelationships(identityRelationshipTable)
        }

        val orderSummaryCdf = ChangeDataFeedViews.orderSummary(cdfTrackerTable, orderSummaryTables)

        val orderSummaryChangeDataResult =
            if (cliArgs.fullReprocess) {
                orderSummaryCdf.queryAllData(CdfConsumerId)
            } else {
                val queryResult = orderSummaryCdf.queryChangeData(CdfConsumerId)
                queryResult.copy(data = queryResult.data.map(_.filter(ChangeTypeColumn === ChangeType.Insert)))
            }

        orderSummaryChangeDataResult.data match {
            case None => logger.info("No updates found since the last run")

            case Some(changeData) =>
                val relatedIdentities = extractRelatedEntities({
                    changeData.filter(ChangeTypeColumn === ChangeType.Insert)
                })

                val identityRelationships = calculateIdentityRelationships(relatedIdentities)
                writeIdentityRelationships(identityRelationships, identityRelationshipTable)

                logger.info(s"Update CDF tracker: ${orderSummaryChangeDataResult.tableMetadataSeq}")
                orderSummaryCdf.markProcessed(orderSummaryChangeDataResult)
        }
    }

    def getCdfTrackerTable(contract: ContractUtils): String = {
        val dataServicesDb = contract.prop[String]("schema.data_services.db_name")
        val cdfTrackerTable = contract.prop[String]("schema.data_services.cdf_tracker_table")
        s"$dataServicesDb.$cdfTrackerTable"
    }

    def getOrderSummaryTables(contract: ContractUtils): Seq[String] = {
        val refinedSquareDb = contract.prop[String]("schema.refined_square.db_name")
        val orderSummaryTable = contract.prop[String]("schema.refined_square.order_summary_table")
        Seq(s"$refinedSquareDb.$orderSummaryTable")
    }

    def getIdentityRelationshipTable(contract: ContractUtils): String = {
        val curatedDb = contract.prop[String]("schema.curated.db_name")
        val identityRelationshipTable = contract.prop[String]("schema.curated.identity_relationship_table")
        s"$curatedDb.$identityRelationshipTable"
    }

    def removeIdentityRelationships(identityRelationshipTable: String)(implicit spark: SparkSession): Unit = {
        spark.sql(s"DELETE FROM $identityRelationshipTable")
    }

    def extractRelatedEntities(orderSummaryDF: DataFrame)(implicit spark: SparkSession): Dataset[RelatedIdentities] = {
        import spark.implicits._

        orderSummaryDF
            .filter(size(col("cxi_identity_id_array")) > 1)
            .select(
                col("cxi_identity_id_array").as("identities"),
                col("ord_date").as("date")
            )
            .as[RelatedIdentities]
    }

    def calculateIdentityRelationships(
                                        relatedIdentitiesDS: Dataset[RelatedIdentities]
                                    )(implicit spark: SparkSession): Dataset[IdentityRelationship] = {
        import spark.implicits._

        relatedIdentitiesDS
            .flatMap({ relatedIdentites =>
                val sortedIdentites = relatedIdentites
                    .identities
                    .sorted(IdentityId.SourceToTargetOrdering)
                    .distinct

                sortedIdentites
                    .combinations(2)
                    .map({ case Seq(sourceIdentity, targetIdentity) =>
                        IdentityRelationship(
                            source = sourceIdentity.cxi_identity_id,
                            source_type = sourceIdentity.customer_type,
                            target = targetIdentity.cxi_identity_id,
                            target_type = targetIdentity.customer_type,
                            relationship = RelationshipType,
                            confidence_score = 0.0,
                            frequency = 1,
                            created_date = relatedIdentites.date,
                            last_seen_date = relatedIdentites.date,
                            active_flag = true
                        )
                    })
            })
            .groupByKey(r => (r.source, r.source_type, r.target, r.target_type))
            .reduceGroups(mergeIdentityRelationships _)
            .map(_._2)
    }

    def writeIdentityRelationships(ds: Dataset[IdentityRelationship], destTable: String): Unit = {
        val df = ds.toDF

        val srcTable = "newIdentityRelationsips"
        df.createOrReplaceTempView(srcTable)

        df.show(false)

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
               |    last_seen_date = ARRAY_MAX(ARRAY($destTable.last_seen_date, $srcTable.last_seen_date))
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    def mergeIdentityRelationships(first: IdentityRelationship, second: IdentityRelationship): IdentityRelationship = {
        first.copy(
            // TODO: merge confidence_scores when it is defined
            frequency = first.frequency + second.frequency,
            created_date = dateOrdering.min(first.created_date, second.created_date),
            last_seen_date = dateOrdering.max(first.last_seen_date, second.last_seen_date)
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
