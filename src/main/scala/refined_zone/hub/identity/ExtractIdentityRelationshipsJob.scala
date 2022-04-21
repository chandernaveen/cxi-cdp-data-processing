package com.cxi.cdp.data_processing
package refined_zone.hub.identity

import java.nio.file.Paths

import com.cxi.cdp.data_processing.refined_zone.hub.identity.model._
import com.cxi.cdp.data_processing.refined_zone.hub.ChangeDataFeedViews
import com.cxi.cdp.data_processing.refined_zone.hub.model.CxiIdentity
import com.cxi.cdp.data_processing.support.SparkSessionFactory
import com.cxi.cdp.data_processing.support.utils.ContractUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import com.cxi.cdp.data_processing.support.change_data_feed.CdfDiffFormat
import org.apache.spark.sql.functions.{array, coalesce, col, explode, flatten, lit, not, struct, when}

/** This Spark job extracts identity relationships from order summary and writes them into identity_relationship table.
  *
  * The job can run in two modes:
  *
  * 1. (default) Incremental processing:
  * - query changed orders (using Change Data Feed)
  * - extract identity relationships
  * - merge them into the identity_relationship table
  *
  * This mode can handle order updates / deletes by creating identity relationships from the previous state
  * of the record with the negative frequency, thus un-applying changes made by the previous state of this record.
  * See `extractRelatedEntities` method for more info.
  *
  * 2. Full reprocess:
  * - remove existing identity relationships
  * - fetch all orders
  * - extract identity relationships
  * - write them into the identity_relationship table
  */
object ExtractIdentityRelationshipsJob {

    final val CdfConsumerId = "extract_identity_relationships_job"

    final val RelationshipType = "posRelated"

    private val logger = Logger.getLogger(this.getClass.getName)

    private val dateOrdering: Ordering[java.sql.Date] = Ordering.by(_.getTime)

    private[identity] case class RelatedIdentities(
                                                      frequency: Int,
                                                      identities: Seq[IdentityId],
                                                      date: java.sql.Date)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    private val OrderSummaryKeys = Seq("cxi_partner_id", "ord_id")

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

        val (changeDataResult, maybeDiffData) =
            if (cliArgs.fullReprocess) {
                val changeDataResult = orderSummaryCdf.queryAllData(CdfConsumerId)
                (changeDataResult, changeDataResult.data.map(CdfDiffFormat.transformRegularDataFrame))
            } else {
                val changeDataResult = orderSummaryCdf.queryChangeData(CdfConsumerId)
                (changeDataResult, changeDataResult.data.map(CdfDiffFormat.transformChangeDataFeed(_, OrderSummaryKeys)))
            }

        maybeDiffData match {
            case None => logger.info("No updates found since the last run")

            case Some(diffData) =>
                val relatedIdentities = extractRelatedEntities(diffData)

                val identityRelationships = createIdentityRelationships(relatedIdentities)
                writeIdentityRelationships(identityRelationships, identityRelationshipTable)

                logger.info(s"Update CDF tracker: ${changeDataResult.tableMetadataSeq}")
                orderSummaryCdf.markProcessed(changeDataResult)
        }
    }

    private def getCdfTrackerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.data_services.db_name")
        val table = contract.prop[String]("schema.data_services.cdf_tracker_table")
        s"$db.$table"
    }

    private def getOrderSummaryTables(contract: ContractUtils): Seq[String] = {
        contract.prop[Seq[String]]("schema.order_summary_tables")
    }

    private def getPosIdentityRelationshipTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.pos_identity_relationship_table")
        s"$db.$table"
    }

    private def removeIdentityRelationships(identityRelationshipTable: String)(implicit spark: SparkSession): Unit = {
        spark.sql(s"DELETE FROM $identityRelationshipTable")
    }

    /** Extracts related identities from the order_summary Change Data Feed dataset transformed into the diff format.
      *
      * The diff format provides the previous (before the changes being processed) and the current state of each order.
      *
      * The interesting cases that require more attention are when the previous_record is not null.
      * It can happen either if an order was updated (then both the previous and the current record will not be null),
      * or if an order was deleted (then the previous record will not be null, but the current record will be null).
      *
      * If the previous record is not null, it means that the identity relationship table was already updated
      * with the identities from this order. So before adding new identity relationships from the current state
      * of this order, we have to remove identity relationships that were added from the previous state of this order.
      * To do this, we generate identity relationships from the previous state of the order, but with the negative
      * frequency (-1).
      *
      * Example:
      * Let's say an order with ord_id == 1 has a list of identities ["phone:111", "phone:222"].
      * POS identity relationship table generated based on this order has a relationship between
      * `phone:111` and `phone:222` with the frequency of 1.
      *
      * Then let's say this order gets updated (perhaps because there was a mistake that got corrected),
      * and now the list of identities looks like ["phone:111", "phone:333"]. Here is the diff for this order:
      * +--------------------------------------+--------------------------------------+
      * | previous_record                      | current_record                       |
      * +--------+-----------------------------+--------+-----------------------------+
      * | ord_id | identities                  | ord_id | identities                  |
      * +--------+-----------------------------+--------+-----------------------------+
      * | 1      | ["phone:111", "phone:222"]  | 1      | ["phone:111", "phone:333"]  |
      * +--------+-----------------------------+--------+-----------------------------+
      *
      * To process these changes, two identity relationship updates are generated:
      * - a relationship between `phone:111` and `phone:222` with the frequency of -1, to remove the previously created
      *   relationship
      * - a relationship between `phone:111` and `phone:333` with the frequency of 1, to add the new relationship
      */
    private[identity] def extractRelatedEntities(orderSummaryDiffDF: DataFrame)(implicit spark: SparkSession): Dataset[RelatedIdentities] = {
        import spark.implicits._

        val ordDateFallback = new java.sql.Date(java.time.Instant.now().toEpochMilli)

        /* due to Spark DataFrame API limitations, extracts optional record as an array to be flattened later */
        def extractOptionalRelatedIdentities(parentColumn: String, frequency: Int): Column = {
            when(col(parentColumn).isNull, array())
                .otherwise(
                    array(
                        struct(
                            lit(frequency).as("frequency"),
                            col(s"$parentColumn.${CxiIdentity.CxiIdentityIds}").as("identities"),
                            coalesce(col(s"$parentColumn.ord_date"), lit(ordDateFallback)).as("date"))))
        }

        filterOrdersWithModifiedIdentities(orderSummaryDiffDF)
            .select(
                explode(
                    flatten(
                        array(
                            extractOptionalRelatedIdentities(
                                CdfDiffFormat.PreviousRecordColumnName,
                                frequency = RemoveRelationshipFrequency),
                            extractOptionalRelatedIdentities(
                                CdfDiffFormat.CurrentRecordColumnName,
                                frequency = AddRelationshipFrequency))))
                    .as("related_identities")
            )
            .select(col("related_identities.*"))
            .as[RelatedIdentities]
            .filter(record => record.identities != null && record.identities.size > 1)
    }

    /** If identities have not been modified, identity relationships stay the same. */
    private[identity] def filterOrdersWithModifiedIdentities(orderSummaryDiffDF: DataFrame): DataFrame = {
        val prevIdentitiesCol = col(s"${CdfDiffFormat.PreviousRecordColumnName}.${CxiIdentity.CxiIdentityIds}")
        val currIdentitiesCol = col(s"${CdfDiffFormat.CurrentRecordColumnName}.${CxiIdentity.CxiIdentityIds}")

        orderSummaryDiffDF.filter(not(prevIdentitiesCol <=> currIdentitiesCol))
    }

    private final val AddRelationshipFrequency = 1
    private final val RemoveRelationshipFrequency = -1

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
                    frequency = relatedIdentities.frequency,
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
               |WHEN MATCHED AND ($destTable.frequency + $srcTable.frequency) > 0
               |  THEN UPDATE SET
               |    frequency = $destTable.frequency + $srcTable.frequency,
               |    created_date = ARRAY_MIN(ARRAY($destTable.created_date, $srcTable.created_date)),
               |    last_seen_date = ARRAY_MAX(ARRAY($destTable.last_seen_date, $srcTable.last_seen_date)),
               |    active_flag = $destTable.active_flag AND $srcTable.active_flag
               |WHEN MATCHED AND ($destTable.frequency + $srcTable.frequency) <= 0
               |  THEN DELETE
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
                .text("if true, remove current identity relationships and reprocess them from the beginning")

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser.parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
