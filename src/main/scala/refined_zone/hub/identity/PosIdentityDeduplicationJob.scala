package com.cxi.cdp.data_processing
package refined_zone.hub.identity

import refined_zone.hub.model.CxiIdentity.{CxiIdentityId, Metadata, Type, Weight}
import support.change_data_feed.{CdfDiffFormat, ChangeDataFeedService, ChangeDataFeedSource}
import support.change_data_feed.CdfDiffFormat.{CurrentRecordColumnName, PreviousRecordColumnName}
import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import scopt.OptionParser

import java.nio.file.Paths

object PosIdentityDeduplicationJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    private final val CdfConsumerId = "pos_identity_deduplication_job"
    private val IdentityCompositeKeyColumns = Seq(CxiIdentityId, Type)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        run(cliArgs)(SparkSessionFactory.getSparkSession())
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get("/mnt/" + cliArgs.contractPath))

        val cdfTrackerTable = getCdfTrackerTable(contract)
        val posIdentityIntermediateTable = getPosIdentityIntermediateTable(contract)
        val posIdentityFinalTable = getPosIdentityTable(contract)

        val cdfService = new ChangeDataFeedService(cdfTrackerTable)
        val posIdentityIntermediateCdf = new ChangeDataFeedSource.SingleTable(cdfService, posIdentityIntermediateTable)

        val (changeDataResult, maybeDiffData) =
            if (cliArgs.fullReprocess) {
                val changeDataResult = posIdentityIntermediateCdf.queryAllData(CdfConsumerId)
                (changeDataResult, changeDataResult.data.map(CdfDiffFormat.transformRegularDataFrame))
            } else {
                val changeDataResult = posIdentityIntermediateCdf.queryChangeData(CdfConsumerId)
                (
                    changeDataResult,
                    changeDataResult.data.map(CdfDiffFormat.transformChangeDataFeed(_, IdentityCompositeKeyColumns))
                )
            }

        maybeDiffData match {
            case None => logger.info("No updates found since the last run")
            case Some(diffData) =>
                process(diffData, posIdentityFinalTable)
                logger.info(s"Update CDF tracker: ${changeDataResult.tableMetadataSeq}")
                posIdentityIntermediateCdf.markProcessed(changeDataResult)
        }

    }

    def process(diffData: DataFrame, posIdentityFinalTable: String): Unit = {
        val (deletedIndentities, newOrUpdatedIdentities) = transform(diffData)
        writeIdentities(deletedIndentities, newOrUpdatedIdentities, posIdentityFinalTable)
    }

    def transform(diffData: DataFrame): (DataFrame, DataFrame) = {

        val deletedIdentities = diffData
            .where(col(CurrentRecordColumnName).isNull and col(PreviousRecordColumnName).isNotNull)
            .select(s"$PreviousRecordColumnName.$CxiIdentityId", s"$PreviousRecordColumnName.$Type")
            .dropDuplicates(IdentityCompositeKeyColumns)

        val createdOrUpdatedIdentities = diffData
            .where(col(CurrentRecordColumnName).isNotNull)
            .select(
                s"$CurrentRecordColumnName.$CxiIdentityId",
                s"$CurrentRecordColumnName.$Type",
                s"$CurrentRecordColumnName.$Weight",
                s"$CurrentRecordColumnName.$Metadata"
            )
            .dropDuplicates(IdentityCompositeKeyColumns)

        (deletedIdentities, createdOrUpdatedIdentities)
    }

    def writeIdentities(
        deletedIdentities: DataFrame,
        newOrUpdatedIdentities: DataFrame,
        posIdentityFinalTable: String
    ): Unit = {

        val srcTable = "newIdentitiesTable"

        deletedIdentities.createOrReplaceTempView(srcTable)
        deletedIdentities.sqlContext.sql(s"""
              |MERGE INTO $posIdentityFinalTable
              |USING $srcTable
              |ON $posIdentityFinalTable.cxi_identity_id = $srcTable.cxi_identity_id
              | AND $posIdentityFinalTable.type = $srcTable.type
              |WHEN MATCHED
              |  THEN DELETE
              |""".stripMargin)

        newOrUpdatedIdentities.createOrReplaceTempView(srcTable)
        newOrUpdatedIdentities.sqlContext.sql(s"""
               |MERGE INTO $posIdentityFinalTable
               |USING $srcTable
               |ON $posIdentityFinalTable.cxi_identity_id = $srcTable.cxi_identity_id
               | AND $posIdentityFinalTable.type = $srcTable.type
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

    private def getPosIdentityTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.refined_hub.db_name")
        val table = contract.prop[String]("datalake.refined_hub.pos_identity_table")
        s"$db.$table"
    }

    private def getPosIdentityIntermediateTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.refined_hub.db_name")
        val table = contract.prop[String]("datalake.refined_hub.pos_identity_intermediate_table")
        s"$db.$table"
    }

    private def getCdfTrackerTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("datalake.data_services.db_name")
        val table = contract.prop[String]("datalake.data_services.cdf_tracker_table")
        s"$db.$table"
    }
}

case class CliArgs(contractPath: String, fullReprocess: Boolean = false)

object CliArgs {
    private val initOptions = CliArgs(contractPath = null)

    private def optionsParser: OptionParser[CliArgs] =
        new scopt.OptionParser[CliArgs]("POS Identity Deduplication Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, read all data from intermediate table")
        }

    def parse(args: Seq[String]): CliArgs = {
        optionsParser
            .parse(args, initOptions)
            .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
    }
}
